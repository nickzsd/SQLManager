from typing              import Any, List, Dict, Optional, Union
from ..connection        import database_connection as data, Transaction
from .EDTController      import EDTController
from .BaseEnumController import BaseEnumController

class TableController:
    """
    Classe de controle de tabelas do banco de dados (SQL Server)
    - Todos os campos da tabela devem ser representados como propriedades EDTController ou BaseEnumController na classe filha
    - Todos os métodos CRUD, SELECT e EXISTS são assíncronos (use awaitables se usar async DB, aqui está síncrono para pyodbc)
    - Todos os métodos GETs possuem cache interno
    - Suporta transações isoladas: TableController(db_ou_transaction, "table_name")
    """
    def __init__(self, db: Union[data, Transaction], table_name: str):
        '''
        Inicializa o controlador de tabela.
        Args:
            db (Union[data, Transaction]): Instância de conexão ou transação.
            table_name (str): Nome da tabela no banco de dados.
        '''
        self.db = db
        self.table_name = table_name.upper()
        self.records: List[Dict[str, Any]] = []
        self.Columns: Optional[List[List[Any]]] = None
        self.Indexes: Optional[List[str]] = None
        self.ForeignKeys: Optional[List[Dict[str, Any]]] = None
        self._joins: List[Dict[str, Any]] = []
        self._query_where: Optional[List[Dict[str, Any]]] = None
        self._query_columns: Optional[List[str]] = None
        self._query_options: Optional[Dict[str, Any]] = None

    def __setattr__(self, name: str, value: Any):
        '''
        Intercepta atribuições diretas aos campos EDT/Enum para garantir validação.
        Se o atributo já é um EDT/Enum, redireciona para seu setter value.
        '''
        # Permite atribuição direta para atributos de controle da classe
        if name in ('db', 'table_name', 'records', 'Columns', 'Indexes', 'ForeignKeys'):
            object.__setattr__(self, name, value)
            return

        # Se o atributo já existe e é um EDT/Enum, usa seu setter
        if hasattr(self, name):            
            attr = object.__getattribute__(self, name)
            if isinstance(attr, (EDTController, BaseEnumController)):
                # Se o valor também for EDT/Enum, extrai o .value
                if isinstance(value, EDTController):
                    attr.value = value.value
                elif isinstance(value, (BaseEnumController, BaseEnumController.Enum)):
                    # Para BaseEnumController, aceita tanto a instância quanto o enum member
                    if isinstance(value, BaseEnumController):
                        attr.value = value.value
                    else:
                        # É um enum member direto (ex: ItemType.NoneType)
                        attr.value = value.value
                else:
                    attr.value = value
                return
        # Caso contrário, atribuição normal
        object.__setattr__(self, name, value)

    def __iter__(self):
        '''
        Permite iterar diretamente sobre a instância após select().join()
        Executa a query automaticamente quando usado em um loop.
        '''
        results = self.execute()
        return iter(results)
    
    def __len__(self):
        '''
        Retorna o total de registros quando len() é chamado após select().
        Executa a query automaticamente se necessário.
        '''
        results = self.execute()
        return len(results)
    
    def __getitem__(self, index):
        '''
        Permite acessar resultados por índice após select().
        Executa a query automaticamente se necessário.
        '''
        results = self.execute()
        return results[index]

    def _is_aggregate_function(self, column: str) -> bool:
        '''
        Verifica se a coluna contém uma função de agregação SQL.
        Args:
            column (str): Nome da coluna ou expressão SQL
        Returns:
            bool: True se for uma função de agregação
        '''
        aggregate_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'STRING_AGG']
        column_upper = column.upper().strip()
        return any(func in column_upper for func in aggregate_functions)

    def _extract_field_from_aggregate(self, column: str) -> Optional[str]:
        '''
        Extrai o nome do campo de dentro de uma função de agregação.
        Ex: 'SUM(PRICE)' -> 'PRICE', 'COUNT(*)' -> 'RECID', 'COUNT(1)' -> 'RECID'
        Args:
            column (str): Expressão SQL com função de agregação
        Returns:
            Optional[str]: Nome do campo ou None se não for possível extrair
        '''
        import re
        # Remove espaços e busca padrão FUNC(FIELD) ou FUNC(*) ou FUNC(1)
        match = re.search(r'\([\s]*([A-Za-z_][A-Za-z0-9_]*|\*|\d+)[\s]*\)', column)
        if match:
            field = match.group(1).upper()
            # COUNT(*) e COUNT(1) mapeiam para RECID
            if field in ('*', '1'):
                return 'RECID'
            return field
        return None

    def get_table_columns(self) -> List[List[Any]]:
        '''
        Retorna as colunas da tabela (nome, tipo, se aceita nulo).
        Returns:
            List[List[Any]]: Lista de colunas, cada uma como [nome, tipo, is_nullable].
        '''
        if self.Columns:
            return self.Columns
        query = f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
        rows = self.db.doQuery(query, (self.table_name,))
        self.Columns = [[row[0], row[1], row[2]] for row in rows]
        return self.Columns

    def get_table_index(self) -> List[str]:
        '''
        Retorna os índices da tabela.
        Returns:
            List[str]: Lista com os nomes dos índices.
        '''
        if self.Indexes:
            return self.Indexes
        query = f"SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID(?)"
        rows = self.db.doQuery(query, (self.table_name,))
        self.Indexes = [row[0] for row in rows]
        return self.Indexes

    def get_table_foreign_keys(self) -> List[Dict[str, Any]]:
        '''
        Retorna as chaves estrangeiras relacionadas à tabela.
        Returns:
            List[Dict[str, Any]]: Lista de dicionários com informações das FKs.
        '''
        if self.ForeignKeys:
            return self.ForeignKeys
        query = '''
            SELECT
                fk.name AS f_key,
                tp.name AS t_origin,
                cp.name AS c_origin,
                tr.name AS t_reference,
                cr.name AS c_reference
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
            INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
            INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
            INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
            WHERE tp.name = ? OR tr.name = ?
        '''
        rows = self.db.doQuery(query, (self.table_name, self.table_name))
        self.ForeignKeys = [
            {
                'f_key': row[0],
                't_origin': row[1],
                'c_origin': row[2],
                't_reference': row[3],
                'c_reference': row[4],
            } for row in rows
        ]
        return self.ForeignKeys

    def get_table_total(self) -> int:
        '''
        Retorna o total de registros atualmente carregados na instância.
        Returns:
            int: Total de registros.
        '''        
        return len(self.records)

    def insert(self) -> bool:
        '''
        Insere um novo registro na tabela.
        Returns:
            bool: True se inserido com sucesso, lança Exception caso contrário.
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        ''' 
        REMOVIDO: verficação de recid
        if hasattr(self, 'RECID') and getattr(self, 'RECID').value is not None and getattr(self, 'RECID').value != 0:
            raise Exception("Inserção com dados já existentes, limpe os campos antes de inserir")
        '''
        fields = []
        values = []
        for key in self.__dict__:
            attr = getattr(self, key)
            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            fields.append(key)
            values.append(attr.value)
        if not fields:
            raise Exception("Nenhum campo para inserir")
        query = f"INSERT INTO {self.table_name} (" + ", ".join(fields) + ") OUTPUT INSERTED.RECID VALUES (" + ", ".join(['?'] * len(fields)) + ")"
        try:
            self.validate_write()
            self.db.ttsbegin()
            result = self.db.doQuery(query, tuple(values))
            
            new_recid = int(result[0][0]) if result and result[0][0] else None
            self.db.ttscommit()
                        
            if new_recid is not None:
                self.select([{'field': 'RECID', 'operator': '=', 'value': new_recid}], ['*'], {'doUpdate': True, 'limit': 1})
            
            return True
        except Exception as error:
            self.db.ttsabort()
            raise Exception(f"Erro ao inserir registro: {error}")

    def update(self) -> bool:
        '''
        Atualiza um registro existente na tabela.
        Returns:
            bool: True se atualizado com sucesso, lança Exception caso contrário.
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        if not hasattr(self, 'RECID') or getattr(self, 'RECID').value is None:
            raise Exception("Atualização sem chave primaria, preencha o campo RECID")
        if not self.exists([{'field': 'RECID', 'operator': '=', 'value': getattr(self, 'RECID').value}]):
            raise Exception(f"Registro com RECID {getattr(self, 'RECID').value} não existe na tabela {self.table_name}")
        record = self.select([{'field': 'RECID', 'operator': '=', 'value': getattr(self, 'RECID').value}], ['*'], {'doUpdate': False, 'limit': 1})
        values = []
        set_clauses = []
        for key in self.__dict__:
            attr = getattr(self, key)
            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            if record and (record[0].get(key) == attr.value or record[0].get(key) == getattr(attr, '_value', None)):
                continue
            set_clauses.append(f"{key} = ?")
            values.append(attr.value)
        if not values:
            raise Exception("Nenhum campo foi alterado para atualizar.")
        query = f"UPDATE {self.table_name} SET " + ", ".join(set_clauses) + " WHERE RECID = ?"
        values.append(getattr(self, 'RECID').value)
        try:
            self.validate_write()
            self.db.ttsbegin()
            self.db.executeCommand(query, tuple(values))
            self.db.ttscommit()
            # Atualiza a instância com os dados atuais do banco
            updated_record = self.select([
                {'field': 'RECID', 'operator': '=', 'value': getattr(self, 'RECID').value}
            ], ['*'], {'doUpdate': False, 'limit': 1})
            if updated_record:
                self.set_current(updated_record[0])
            return True
        except Exception as error:
            self.db.ttsabort()
            raise Exception(f"Erro ao atualizar registro: {error}")

    def delete(self) -> bool:
        '''
        Exclui um registro da tabela.
        Returns:
            bool: True se excluído com sucesso, lança Exception caso contrário.
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        if not hasattr(self, 'RECID') or getattr(self, 'RECID').value is None:
            raise Exception("Exclusão sem chave primaria, preencha o campo RECID")
        if not self.exists([{'field': 'RECID', 'operator': '=', 'value': getattr(self, 'RECID').value}]):
            raise Exception(f"Registro com RECID {getattr(self, 'RECID').value} não existe na tabela {self.table_name}")
        query = f"DELETE FROM {self.table_name} WHERE RECID = ?"
        try:
            self.db.ttsbegin()
            self.db.executeCommand(query, (getattr(self, 'RECID').value,))
            self.db.ttscommit()
        except Exception as error:
            self.db.ttsabort()
            raise Exception(f"Erro ao excluir registro: {error}")
        self.clear()
        # Se existir o atributo RECID, zera o valor
        if hasattr(self, 'RECID'):
            getattr(self, 'RECID').value = None
        return True

    def update_recordset(self, set_values: Dict[str, Any], where: Optional[List[Dict[str, Any]]] = None) -> int:
        '''
        Atualiza múltiplos registros em massa (UPDATE ... SET ... WHERE).
        Estilo AX2012: update_recordset table setting field = value where condition.
        Args:
            set_values (Dict[str, Any]): Dicionário com campos e valores. Ex: {'PRICE': 100, 'MODIFIEDDATE': datetime.now()}
            where (Optional[List[Dict[str, Any]]]): Filtros WHERE. Ex: [{'field': 'CATEGORY', 'operator': '=', 'value': 'Electronics'}]
        Returns:
            int: Número de registros afetados
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        if not set_values:
            raise Exception("Nenhum campo para atualizar")
        
        # Valida se os campos existem na tabela
        table_columns = self.get_table_columns()
        col_names = [col[0] for col in table_columns]
        for field in set_values.keys():
            if field.upper() not in col_names:
                raise Exception(f"Campo '{field}' não existe na tabela {self.table_name}")
        
        # Monta query UPDATE
        set_clauses = [f"{field} = ?" for field in set_values.keys()]
        query = f"UPDATE {self.table_name} SET " + ", ".join(set_clauses)
        values = list(set_values.values())
        
        # Adiciona WHERE se houver
        if where:
            where_clauses = []
            for f in where:
                if f['field'] not in col_names:
                    raise Exception(f"Coluna {f['field']} não existe na tabela")
                operator = f.get('operator', '=')
                where_clauses.append(f"{f['field']} {operator} ?")
                values.append(f['value'])
            query += " WHERE " + " AND ".join(where_clauses)
        
        try:
            self.db.ttsbegin()
            cursor = self.db.executeCommand(query, tuple(values))
            affected_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            self.db.ttscommit()
            return affected_rows
        except Exception as error:
            self.db.ttsabort()
            raise Exception(f"Erro ao atualizar registros em massa: {error}")

    def delete_from(self, where: Optional[List[Dict[str, Any]]] = None) -> int:
        '''
        Deleta múltiplos registros em massa (DELETE FROM ... WHERE).
        Estilo AX2012: delete_from table where condition.
        Args:
            where (Optional[List[Dict[str, Any]]]): Filtros WHERE. Ex: [{'field': 'CATEGORY', 'operator': '=', 'value': 'Obsolete'}]
        Returns:
            int: Número de registros deletados
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        # Valida WHERE
        table_columns = self.get_table_columns()
        col_names = [col[0] for col in table_columns]
        
        query = f"DELETE FROM {self.table_name}"
        values = []
        
        if where:
            where_clauses = []
            for f in where:
                if f['field'] not in col_names:
                    raise Exception(f"Coluna {f['field']} não existe na tabela")
                operator = f.get('operator', '=')
                where_clauses.append(f"{f['field']} {operator} ?")
                values.append(f['value'])
            query += " WHERE " + " AND ".join(where_clauses)
        else:
            raise Exception("DELETE sem WHERE não é permitido. Use where=[] explicitamente se desejar deletar tudo.")
        
        try:
            self.db.ttsbegin()
            cursor = self.db.executeCommand(query, tuple(values))
            affected_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            self.db.ttscommit()
            return affected_rows
        except Exception as error:
            self.db.ttsabort()
            raise Exception(f"Erro ao deletar registros em massa: {error}")

    def insert_recordset(self, columns: List[str], source_data: List[tuple]) -> int:
        '''
        Insere múltiplos registros em massa (INSERT INTO ... VALUES (...), (...)).
        Estilo AX2012: insert_recordset destTable (fields) select/values.
        Args:
            columns (List[str]): Lista de colunas a inserir. Ex: ['ITEMID', 'ITEMNAME', 'PRICE']
            source_data (List[tuple]): Lista de tuplas com valores. Ex: [('ITEM001', 'Product 1', 100), ('ITEM002', 'Product 2', 200)]
        Returns:
            int: Número de registros inseridos
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        if not columns or not source_data:
            raise Exception("Colunas e dados são obrigatórios para insert_recordset")
        
        # Valida se os campos existem na tabela
        table_columns = self.get_table_columns()
        col_names = [col[0] for col in table_columns]
        for col in columns:
            if col.upper() not in col_names:
                raise Exception(f"Campo '{col}' não existe na tabela {self.table_name}")
        
        # Valida se todas as tuplas têm o mesmo número de valores
        expected_len = len(columns)
        for idx, row in enumerate(source_data):
            if len(row) != expected_len:
                raise Exception(f"Linha {idx} tem {len(row)} valores, esperado {expected_len}")
        
        # Monta query INSERT com múltiplos VALUES
        placeholders = ', '.join(['?'] * len(columns))
        values_clause = ', '.join([f"({placeholders})" for _ in source_data])
        query = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES {values_clause}"
        
        # Flatten dos valores
        flat_values = [val for row in source_data for val in row]
        
        try:
            self.db.ttsbegin()
            cursor = self.db.executeCommand(query, tuple(flat_values))
            affected_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else len(source_data)
            self.db.ttscommit()
            return affected_rows
        except Exception as error:
            self.db.ttsabort()
            raise Exception(f"Erro ao inserir registros em massa: {error}")

    def select(self, where: Optional[List[Dict[str, Any]]] = None, columns: Optional[List[str]] = None, options: Optional[Dict[str, Any]] = None):
        '''
        Configura ou executa um SELECT na tabela.
        Se for encadeado com join(), retorna self. Caso contrário, executa automaticamente.
        
        Args:
            where (Optional[List[Dict[str, Any]]]): Filtros para o SELECT. Ex: [{'field': 'NOME', 'operator': '=', 'value': 'Joao'}]
            columns (Optional[List[str]]): Colunas a serem retornadas. Default: ['*']. Suporta funções como COUNT(*), SUM(campo), etc.
            options (Optional[Dict[str, Any]]): Opções como orderBy, limit, offset, doUpdate, groupBy, having, distinct.
                - groupBy: Lista de campos para GROUP BY. Ex: ['ITEMID', 'CATEGORY']
                - having: Lista de condições para HAVING. Ex: [{'field': 'COUNT(*)', 'operator': '>', 'value': 5}]
                - distinct: Se True, adiciona DISTINCT na query. Ex: {'distinct': True}
        Returns:
            self: Retorna self para encadeamento com join()
        '''
        # Armazena os parâmetros da query
        self._query_where = where
        self._query_columns = columns
        self._query_options = options
        
        # Retorna self para permitir encadeamento com .join()
        return self
    
    def execute(self) -> List[Any]:
        '''
        Executa a query SELECT configurada com os parâmetros e joins armazenados.
        Returns:
            List[Any]: Lista de instâncias das tabelas envolvidas (se houver JOINs), ou dicts (sem JOIN).
        '''
        validate = self.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        where = self._query_where
        columns = self._query_columns or ['*']
        options = self._query_options or {}
        
        order_by = options.get('orderBy')
        limit = options.get('limit', 100)
        offset = options.get('offset', 0)
        do_update = options.get('doUpdate', True)
        group_by = options.get('groupBy')
        having = options.get('having')
        distinct = options.get('distinct', False)
        table_columns = self.get_table_columns()
        has_aggregates = any(self._is_aggregate_function(col) for col in columns) if columns != ['*'] else False
        
        # Validação de colunas
        if columns != ['*']:
            col_names = [col[0] for col in table_columns]
            for col in columns:
                if self._is_aggregate_function(col):
                    # Valida se o campo dentro da função existe
                    field_name = self._extract_field_from_aggregate(col)
                    if field_name and field_name not in col_names:
                        raise Exception(f"Campo '{field_name}' na agregação '{col}' não existe na tabela")
                elif col not in col_names:
                    raise Exception(f"Coluna inválida: {col}")
        # Monta SELECT com JOINs se houver
        main_alias = self.table_name
        select_columns = []
        if columns == ['*']:
            select_columns += [f"{main_alias}.{col[0]} AS {main_alias}_{col[0]}" for col in table_columns]
        else:
            for col in columns:
                if self._is_aggregate_function(col):
                    # Função de agregação: não adiciona alias de tabela antes, apenas AS no final
                    # Extrai um nome limpo para o alias (ex: COUNT(*) -> COUNT_ALL)
                    alias_name = col.replace('(', '_').replace(')', '').replace('*', 'ALL').replace('.', '_').replace(' ', '')
                    select_columns.append(f"{col} AS {alias_name}")
                else:
                    # Coluna normal: adiciona alias da tabela
                    select_columns.append(f"{main_alias}.{col} AS {main_alias}_{col}")
        join_clauses = []
        join_controllers = []
        for join in getattr(self, '_joins', []):
            ctrl = join['controller']
            alias = join['alias']
            join_type = join['type']
            join_on = join['on']
            index_hint = join.get('index_hint')
            join_columns = ctrl.get_table_columns()
            join_controllers.append((ctrl, alias))
            if join['columns']:
                select_columns += [f"{alias}.{col} AS {alias}_{col}" for col in join['columns']]
            else:
                select_columns += [f"{alias}.{col[0]} AS {alias}_{col[0]}" for col in join_columns]
            hint = f" WITH (INDEX({index_hint}))" if index_hint else ""
            join_clauses.append(f" {join_type} JOIN {ctrl.table_name} AS {alias}{hint} ON {' '.join(join_on)} ")
        
        distinct_keyword = "DISTINCT " if distinct else ""
        query = f"SELECT {distinct_keyword}{', '.join(select_columns)} FROM {self.table_name} AS {main_alias}" + ''.join(join_clauses)
        values = []
        if where:
            where_clauses = []
            for idx, f in enumerate(where):
                if f['field'] not in [col[0] for col in table_columns]:
                    raise Exception(f"Coluna {f['field']} não existe na tabela")
                operator = f.get('operator', '=')
                logical = f.get('logical', None)
                clause = f"{main_alias}.{f['field']} {operator} ?"
                if idx > 0:
                    prev_logical = where[idx-1].get('logical', 'AND').upper() if 'logical' in where[idx-1] else 'AND'
                    clause = f"{prev_logical} {clause}"
                where_clauses.append(clause)
                values.append(f['value'])
            query += " WHERE " + " ".join(where_clauses)
        
        # GROUP BY
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            group_clauses = [f"{main_alias}.{field}" for field in group_by]
            query += " GROUP BY " + ", ".join(group_clauses)
        
        # HAVING
        if having:
            having_clauses = []
            for h in having:
                operator = h.get('operator', '=')
                having_clauses.append(f"{h['field']} {operator} ?")
                values.append(h['value'])
            query += " HAVING " + " AND ".join(having_clauses)
        
        if order_by:
            query += f" ORDER BY {main_alias}.{order_by}"
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        rows = self.db.doQuery(query, tuple(values))
        
        # Se houver agregações, mapeia resultados para instâncias quando possível
        if has_aggregates or group_by:
            # Monta mapeamento coluna SQL -> campo EDT
            column_mapping = []  # [(sql_index, field_name, is_aggregate)]
            sql_idx = 0
            
            if columns == ['*']:
                for col in table_columns:
                    column_mapping.append((sql_idx, col[0], False))
                    sql_idx += 1
            else:
                for col in columns:
                    if self._is_aggregate_function(col):
                        # Tenta extrair o nome do campo da função
                        field_name = self._extract_field_from_aggregate(col)
                        if field_name:
                            column_mapping.append((sql_idx, field_name, True))
                        else:
                            # COUNT(*) ou similar - adiciona como atributo especial
                            alias_name = col.replace('(', '_').replace(')', '').replace('*', 'ALL').replace('.', '_').replace(' ', '')
                            column_mapping.append((sql_idx, alias_name, True))
                        sql_idx += 1
                    else:
                        column_mapping.append((sql_idx, col, False))
                        sql_idx += 1
            
            # Processa resultados
            results = []
            for row in rows:
                # Cria instância da tabela principal
                main_instance = self.__class__(self.db)
                aggregate_extras = {}  # Para COUNT(*) e similares
                
                for sql_idx, field_name, is_agg in column_mapping:
                    value = row[sql_idx]
                    if hasattr(main_instance, field_name):
                        # Campo EDT existe - atribui o valor
                        getattr(main_instance, field_name).value = value
                    else:
                        # Campo não existe (ex: COUNT_ALL) - guarda em dict
                        aggregate_extras[field_name] = value
                
                # Adiciona extras como atributo especial
                if aggregate_extras:
                    main_instance._aggregate_results = aggregate_extras
                
                results.append(main_instance)
            
            if do_update and results:
                self.records = results
                self.set_current(results[0])
            
            # Limpa os parâmetros da query após execução
            self._query_where = None
            self._query_columns = None
            self._query_options = None
            self._joins = []
            
            return results
        
        # Se houver JOINs (sem agregações), retorna instâncias das tabelas
        if self._joins:
            results = []
            for row in rows:
                idx = 0
                # Monta instância da tabela principal
                main_data = {}
                for col in table_columns:
                    main_data[col[0]] = row[idx]
                    idx += 1
                main_instance = self.__class__(self.db)
                main_instance.set_current(main_data)
                join_instances = []
                for ctrl, alias in join_controllers:
                    join_cols = ctrl.get_table_columns()
                    join_data = {}
                    for col in join_cols:
                        join_data[col[0]] = row[idx]
                        idx += 1
                    join_instance = ctrl.__class__(ctrl.db)
                    join_instance.set_current(join_data)
                    join_instances.append(join_instance)
                results.append([main_instance] + join_instances)
            if do_update and results:
                self.records = [r[0] for r in results]
                self.set_current(results[0][0])
            
            # Limpa os parâmetros da query após execução
            self._query_where = None
            self._query_columns = None
            self._query_options = None
            self._joins = []
            
            return results
        else:
            result = [dict(zip([col[0] for col in table_columns], row)) for row in rows]
            if do_update and result:
                self.records = result
                self.set_current(result[0])
            
            # Limpa os parâmetros da query após execução
            self._query_where = None
            self._query_columns = None
            self._query_options = None
            
            return result
    
    def join(self, other_table_controller, on: list, join_type: str = 'INNER', columns: Optional[list] = None, alias: Optional[str] = None, index_hint: Optional[str] = None):
        '''
        Adiciona um JOIN encadeado à query de forma dinâmica, usando pares de atributos das instâncias.
        Aceita:
        - Lista de tuplas: [(campo1, campo2), ...] para AND implícito
        - Lista de dicts: [{'field': (campo1, campo2), 'operator': '=', 'logical': 'OR'}, ...]
        '''
        main_alias = self.table_name
        other_alias = alias or other_table_controller.table_name
        on_clauses = []
        
        # Helper para extrair o nome do campo do EDT/Enum
        def get_field_name(attr_obj, controller):
            # Procura o nome do atributo na instância do controller
            for attr_name in controller.__dict__:
                if getattr(controller, attr_name) is attr_obj:
                    return attr_name
            # Fallback: usa o nome da classe (menos confiável)
            return attr_obj.__class__.__name__.upper()
        
        # Suporte ao novo formato (lista de dicts)
        if on and isinstance(on[0], dict):
            for idx, cond in enumerate(on):
                field = cond['field']
                operator = cond.get('operator', '=')
                # field deve ser uma tupla (left, right)
                left, right = field
                left_field = get_field_name(left, self)
                right_field = get_field_name(right, other_table_controller)
                clause = f"{main_alias}.{left_field} {operator} {other_alias}.{right_field}"
                if idx > 0 and 'logical' in on[idx-1]:
                    prev_logical = on[idx-1]['logical'].upper()
                    on_clauses.append(f"{prev_logical} {clause}")
                else:
                    if idx > 0:
                        on_clauses.append(f"AND {clause}")
                    else:
                        on_clauses.append(clause)
        else:
            # Compatibilidade com formato antigo (lista de tuplas)
            for idx, cond in enumerate(on):
                if len(cond) == 2:
                    left, right = cond
                    logical = ''
                elif len(cond) == 3:
                    logical, left, right = cond
                    logical = logical.upper()
                    if logical not in ('AND', 'OR'):
                        raise Exception(f"Operador lógico inválido no JOIN: {logical}")
                else:
                    raise Exception("Cada condição do parâmetro 'on' deve ter 2 ou 3 elementos.")
                
                left_field = get_field_name(left, self)
                right_field = get_field_name(right, other_table_controller)
                clause = f"{main_alias}.{left_field} = {other_alias}.{right_field}"
                
                if idx > 0:
                    # Se tem logical definido (de tupla com 3 elementos), usa ele
                    # Senão, usa AND por padrão
                    prefix = logical if logical else 'AND'
                    on_clauses.append(f"{prefix} {clause}")
                else:
                    on_clauses.append(clause)
        
        # Adiciona o JOIN completo após processar todas as condições ON
        self._joins.append({
            'controller': other_table_controller,
            'on': on_clauses,
            'type': join_type.upper(),
            'columns': columns,
            'alias': other_alias,
            'index_hint': index_hint
        })
        return self

    def exists(self, where: Optional[List[Dict[str, Any]]] = None, columns: Optional[List[str]] = None) -> bool:
        '''
        Verifica se existem registros que atendem aos critérios especificados.
        Args:
            where (Optional[List[Dict[str, Any]]]): Filtros para o SELECT.
            columns (Optional[List[str]]): Colunas a serem verificadas. Default: ['RECID']
        Returns:
            bool: True se existir pelo menos um registro, False caso contrário.
        '''
        rows = self.select(where, columns or ['RECID'], {'doUpdate': False, 'limit': 1})
        return len(rows) > 0

    def validate_fields(self) -> Dict[str, Any]:
        '''
        Valida se os campos da instância existem na tabela.
        Returns:
            Dict[str, Any]: {'valid': True/False, 'error': mensagem}
        '''
        ret = {'valid': True, 'error': ''}
        instance_fields = [k for k in self.__dict__ if isinstance(getattr(self, k), (EDTController, BaseEnumController, BaseEnumController.Enum))]
        table_columns = self.get_table_columns()
        field_names = [col[0].upper() for col in table_columns]
        invalid_fields = [f for f in instance_fields if f.upper() not in field_names]
        if invalid_fields:
            ret = {
                'valid': False,
                'error': f"Campo(s) inválido(s) na instância: [{', '.join(invalid_fields)}] não existem na tabela [{self.table_name}]"
            }
        return ret

    def validate_write(self) -> Dict[str, Any]:
        '''
        Validação antes do insert ou update.
        Verifica se campos obrigatórios estão preenchidos.
        Returns:
            Dict[str, Any]: {'valid': True/False, 'error': mensagem}
        '''
        ret = {'valid': True, 'error': ''}
        columns = self.get_table_columns()
        required_fields = [col[0] for col in columns if col[2] == 'NO' and col[0] != 'RECID']
        instance_fields = {k: getattr(self, k) for k in self.__dict__ if isinstance(getattr(self, k), (EDTController, BaseEnumController, BaseEnumController.Enum))}
        
        for field in required_fields:
            if field not in instance_fields:
                ret = {'valid': False, 'error': f"Campo obrigatório '{field}' não existe na instância"}
                return ret
            attr = instance_fields[field]
            if attr.value is None or attr.value == '':
                ret = {'valid': False, 'error': f"Campo obrigatório '{field}' não pode ser vazio"}
                return ret
        return ret

    def clear(self):
        '''
        Limpa os campos da tabela (seta todos para None) e limpa os registros.
        '''
        for key in self.__dict__:
            attr = getattr(self, key)
            if isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                attr.set_value(None)
        self.records = []

    def set_current(self, record):
        '''
        Preenche os campos da tabela com os valores do banco.
        Args:
            record (Dict[str, Any] | TableController): Linha vinda do banco (SELECT) ou outra instância
        Returns:
            self: Instância preenchida
        '''
        # Se for outra instância de TableController, copia os valores dos EDTs/Enums
        if isinstance(record, TableController):
            for key in self.__dict__:
                if isinstance(getattr(self, key), (EDTController, BaseEnumController, BaseEnumController.Enum)):
                    if hasattr(record, key):
                        source_attr = getattr(record, key)
                        if isinstance(source_attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                            getattr(self, key).value = source_attr.value
            return self
        
        # Se for um dicionário, processa normalmente
        for key, value in record.items():
            if hasattr(self, key):
                attr = getattr(self, key)
                if isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                    attr.value = value
                else:
                    setattr(self, key, value)
        return self

# Utilitário para checagem de colunas
class CheckParms:
    @staticmethod
    def check_columns(fields_table: List[Any], fields_parms: List[str]) -> bool:
        '''
        Verifica se os campos enviados via parâmetro existem na tabela.
        Args:
            fields_table (List[Any]): Campos da tabela (ex: [['NOME', ...], ...])
            fields_parms (List[str]): Campos enviados via parâmetro
        Returns:
            bool: True se todos os campos existem na tabela, False caso contrário
        '''
        if isinstance(fields_parms, str):
            fields_parms = [fields_parms]
        field_names = [f[0] if isinstance(f, (list, tuple)) else f for f in fields_table]
        for field in fields_parms:
            if field.upper() not in [f.upper() for f in field_names]:
                return False
        return True
