from typing              import Any, List, Dict, Optional, Union, Callable
from functools           import wraps
from ..connection        import database_connection as data, Transaction
from .EDTController      import EDTController
from .BaseEnumController import BaseEnumController

class FieldCondition:
    '''
    Representa uma condição de campo com operador para construção de WHERE clauses
    '''
    def __init__(self, field_name: str, operator: str, value: Any, table_alias: Optional[str] = None):
        self.field_name = field_name
        self.operator = operator
        self.value = value
        self.table_alias = table_alias
    
    def __and__(self, other: 'FieldCondition') -> 'BinaryExpression':
        return BinaryExpression(self, 'AND', other)
    
    def __or__(self, other: 'FieldCondition') -> 'BinaryExpression':
        return BinaryExpression(self, 'OR', other)
    
    def to_sql(self) -> tuple:
        '''Converte a condição para SQL'''
        prefix = f"{self.table_alias}." if self.table_alias else ""
        sql = f"{prefix}{self.field_name} {self.operator} ?"
        return (sql, self.value)

class BinaryExpression:
    '''Representa uma expressão binária entre condições'''
    def __init__(self, left: Union[FieldCondition, 'BinaryExpression'], 
                 operator: str, 
                 right: Union[FieldCondition, 'BinaryExpression']):
        self.left = left
        self.operator = operator
        self.right = right
    
    def __and__(self, other: Union[FieldCondition, 'BinaryExpression']) -> 'BinaryExpression':
        return BinaryExpression(self, 'AND', other)
    
    def __or__(self, other: Union[FieldCondition, 'BinaryExpression']) -> 'BinaryExpression':
        return BinaryExpression(self, 'OR', other)
    
    def to_sql(self) -> tuple:
        '''Converte a expressão para SQL recursivamente'''
        left_sql, left_val = self.left.to_sql()
        right_sql, right_val = self.right.to_sql()
        
        left_values = left_val if isinstance(left_val, list) else [left_val]
        right_values = right_val if isinstance(right_val, list) else [right_val]
        
        sql = f"({left_sql} {self.operator} {right_sql})"
        values = left_values + right_values
        
        return (sql, values)

class SelectManager:
    '''Gerencia operações SELECT com API fluente'''
    
    def __init__(self, table_controller):
        self._controller = table_controller
        self._where_conditions: Optional[Union[FieldCondition, BinaryExpression]] = None
        self._columns:          Optional[List[str]] = None
        self._joins:    List[Dict[str, Any]] = []
        self._order_by: Optional[str] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._group_by: Optional[List[str]] = None
        self._having_conditions: Optional[List[Dict[str, Any]]] = None
        self._distinct: bool = False
        self._do_update: bool = True

    def __get__(self, instance, owner=None):
        self._controller = instance
        return self

    def __iter__(self):
        """Permite iterar sobre os resultados"""
        return iter(self.execute())
    
    def __len__(self):
        """Retorna o total de resultados"""
        return len(self.execute())
    
    def __getitem__(self, index):
        """Permite acesso por índice"""
        return self.execute()[index]

    def where(self, condition: Union[FieldCondition, BinaryExpression]) -> 'SelectManager':
        '''Adiciona condições WHERE'''
        self._where_conditions = condition
        return self
    
    def columns(self, *cols: Union[str, EDTController, 'BaseEnumController']) -> 'SelectManager':
        '''Define as colunas a serem retornadas'''
        col_names = []
        for col in cols:
            if isinstance(col, (EDTController, BaseEnumController)):
                col_names.append(col._get_field_name())
            else:
                col_names.append(col)
        self._columns = col_names
        return self
    
    def join(self, other_table, join_type: str = 'INNER') -> 'JoinBuilder':
        '''Inicia um JOIN com outra tabela'''
        return JoinBuilder(self, other_table, join_type)
    
    def order_by(self, column: Union[str, EDTController, 'BaseEnumController']) -> 'SelectManager':
        '''Define ordenação'''
        if isinstance(column, (EDTController, BaseEnumController)):
            self._order_by = column._get_field_name()
        else:
            self._order_by = column
        return self
    
    def limit(self, count: int) -> 'SelectManager':
        '''Define limite de registros'''
        self._limit = count
        return self
    
    def offset(self, count: int) -> 'SelectManager':
        '''Define offset'''
        self._offset = count
        return self
    
    def group_by(self, *columns: Union[str, EDTController, 'BaseEnumController']) -> 'SelectManager':
        '''Define GROUP BY'''
        col_names = []
        for col in columns:
            if isinstance(col, (EDTController, BaseEnumController)):
                col_names.append(col._get_field_name())
            else:
                col_names.append(col)
        self._group_by = col_names
        return self
    
    def having(self, conditions: List[Dict[str, Any]]) -> 'SelectManager':
        '''Define HAVING para usar com GROUP BY'''
        self._having_conditions = conditions
        return self
    
    def distinct(self) -> 'SelectManager':
        '''Adiciona DISTINCT'''
        self._distinct = True
        return self
    
    def do_update(self, update: bool = True) -> 'SelectManager':
        '''Define se deve atualizar a instância com o resultado'''
        self._do_update = update
        return self
    
    def execute(self) -> List[Any]:
        """Executa a query SELECT e retorna resultados (atualiza a instância automaticamente)"""
        validate = self._controller.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        columns = self._columns or ['*']
        limit = self._limit or 100
        offset = self._offset or 0
        
        table_columns = self._controller.get_table_columns()
        has_aggregates = any(self._controller._is_aggregate_function(col) for col in columns) if columns != ['*'] else False
        
        if columns != ['*']:
            col_names = [col[0] for col in table_columns]
            for col in columns:
                if self._controller._is_aggregate_function(col):
                    field_name = self._controller._extract_field_from_aggregate(col)
                    if field_name and field_name not in col_names:
                        raise Exception(f"Campo '{field_name}' na agregação '{col}' não existe na tabela")
                elif col not in col_names:
                    raise Exception(f"Coluna inválida: {col}")
        
        main_alias = self._controller.table_name
        select_columns = []
        
        if columns == ['*']:
            select_columns += [f"{main_alias}.{col[0]} AS {main_alias}_{col[0]}" for col in table_columns]
        else:
            for col in columns:
                if self._controller._is_aggregate_function(col):
                    alias_name = col.replace('(', '_').replace(')', '').replace('*', 'ALL').replace('.', '_').replace(' ', '')
                    select_columns.append(f"{col} AS {alias_name}")
                else:
                    select_columns.append(f"{main_alias}.{col} AS {main_alias}_{col}")
        
        join_clauses = []
        join_controllers = []
        for join in self._joins:
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
            join_clauses.append(f" {join_type} JOIN {ctrl.table_name} AS {alias}{hint} ON {join_on} ")
        
        distinct_keyword = "DISTINCT " if self._distinct else ""
        query = f"SELECT {distinct_keyword}{', '.join(select_columns)} FROM {self._controller.table_name} AS {main_alias}" + ''.join(join_clauses)
        values = []
        
        if self._where_conditions:
            where_sql, where_values = self._where_conditions.to_sql()
            query += f" WHERE {where_sql}"
            values.extend(where_values if isinstance(where_values, list) else [where_values])
        
        if self._group_by:
            group_clauses = [f"{main_alias}.{field}" for field in self._group_by]
            query += " GROUP BY " + ", ".join(group_clauses)
        
        if self._having_conditions:
            having_clauses = []
            for h in self._having_conditions:
                operator = h.get('operator', '=')
                having_clauses.append(f"{h['field']} {operator} ?")
                values.append(h['value'])
            query += " HAVING " + " AND ".join(having_clauses)
        
        if self._order_by:
            query += f" ORDER BY {main_alias}.{self._order_by}"
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        
        rows = self._controller.db.doQuery(query, tuple(values))
        
        if has_aggregates or self._group_by:
            results = self._process_aggregate_results(rows, columns, table_columns)
        elif self._joins:
            results = self._process_join_results(rows, table_columns, join_controllers)
        else:
            results = self._process_simple_results(rows, table_columns)
        
        if self._do_update and results:
            if self._joins:
                self._controller.records = [r[0] for r in results] if results and isinstance(results[0], list) else results
                if results and isinstance(results[0], list):
                    self._controller.set_current(results[0][0])
                elif results:
                    self._controller.set_current(results[0])
            else:
                self._controller.records = results
                self._controller.set_current(results[0])
        
        return results
    
    def _process_aggregate_results(self, rows, columns, table_columns):
        """Processa resultados com agregações"""
        column_mapping = []
        sql_idx = 0
        
        if columns == ['*']:
            for col in table_columns:
                column_mapping.append((sql_idx, col[0], False))
                sql_idx += 1
        else:
            for col in columns:
                if self._controller._is_aggregate_function(col):
                    field_name = self._controller._extract_field_from_aggregate(col)
                    if field_name:
                        column_mapping.append((sql_idx, field_name, True))
                    else:
                        alias_name = col.replace('(', '_').replace(')', '').replace('*', 'ALL').replace('.', '_').replace(' ', '')
                        column_mapping.append((sql_idx, alias_name, True))
                    sql_idx += 1
                else:
                    column_mapping.append((sql_idx, col, False))
                    sql_idx += 1
        
        results = []
        for row in rows:
            main_instance = self._controller.__class__(self._controller.db)
            aggregate_extras = {}
            
            for sql_idx, field_name, is_agg in column_mapping:
                value = row[sql_idx]
                if hasattr(main_instance, field_name):
                    getattr(main_instance, field_name).value = value
                else:
                    aggregate_extras[field_name] = value
            
            if aggregate_extras:
                main_instance._aggregate_results = aggregate_extras
            
            results.append(main_instance)
        
        return results
    
    def _process_join_results(self, rows, table_columns, join_controllers):
        """Processa resultados com JOINs"""
        results = []
        for row in rows:
            idx = 0
            main_data = {}
            for col in table_columns:
                main_data[col[0]] = row[idx]
                idx += 1
            
            main_instance = self._controller.__class__(self._controller.db)
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
        
        return results
    
    def _process_simple_results(self, rows, table_columns):
        """Processa resultados simples sem JOINs"""
        result = [dict(zip([col[0] for col in table_columns], row)) for row in rows]
        return result
    
class JoinBuilder:
    """
    Builder para construir JOINs de forma fluente
    Ex: .join(outra).on(tabela.c.id == outra.c.id)
    """
    def __init__(self, select_manager: SelectManager, other_table, join_type: str):
        self.select_manager = select_manager
        self.other_table = other_table
        self.join_type = join_type
    
    def on(self, condition: Union[FieldCondition, BinaryExpression], 
           columns: Optional[List[str]] = None, 
           alias: Optional[str] = None,
           index_hint: Optional[str] = None) -> SelectManager:
        """
        Define a condição ON do JOIN
        Ex: .on(tabela.c.id == outra.c.id)
        """
        other_alias = alias or self.other_table.table_name
        
        on_sql, _ = condition.to_sql()
        
        self.select_manager._joins.append({
            'controller': self.other_table,
            'on': on_sql,
            'type': self.join_type.upper(),
            'columns': columns,
            'alias': other_alias,
            'index_hint': index_hint
        })
        
        return self.select_manager

class InsertManager:
    """
    Gerencia operações INSERT com validação automática
    """
    
    @staticmethod
    def validate_insert(func: Callable) -> Callable:
        '''Decorator para validar operações de INSERT'''
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            validate = self.validate_fields()
            if not validate['valid']:
                raise Exception(validate['error'])
            
            validate_write = self.validate_write()
            if not validate_write['valid']:
                raise Exception(validate_write['error'])
            
            return func(self, *args, **kwargs)
        return wrapper

    @validate_insert
    def insert(controller) -> bool:
        """
        Insere um novo registro na tabela
        Returns:
            bool: True se inserido com sucesso
        """
        fields = []
        values = []
        
        for key in controller.__dict__:
            attr = controller._get_field_instance(key)
            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            fields.append(key)
            values.append(attr.value)
        
        if not fields:
            raise Exception("Nenhum campo para inserir")
        
        query = f"INSERT INTO {controller.table_name} (" + ", ".join(fields) + ") OUTPUT INSERTED.RECID VALUES (" + ", ".join(['?'] * len(fields)) + ")"
        
        try:
            controller.db.ttsbegin()
            result = controller.db.doQuery(query, tuple(values))
            
            new_recid = int(result[0][0]) if result and result[0][0] else None
            controller.db.ttscommit()
            
            if new_recid is not None:
                recid_instance = controller._get_field_instance('RECID')
                results = controller.select().where(recid_instance == new_recid).limit(1).do_update(True).execute()
            
            return True
        except Exception as error:
            controller.db.ttsabort()
            raise Exception(f"Erro ao inserir registro: {error}")
    
    def insert_recordset(controller, columns: List[str], source_data: List[tuple]) -> int:
        """
        Insere múltiplos registros em massa
        Args:
            columns: Lista de colunas
            source_data: Lista de tuplas com valores
        Returns:
            int: Número de registros inseridos
        """
        validate = controller.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        if not columns or not source_data:
            raise Exception("Colunas e dados são obrigatórios para insert_recordset")
        
        table_columns = controller.get_table_columns()
        col_names = [col[0] for col in table_columns]
        
        for col in columns:
            if col.upper() not in col_names:
                raise Exception(f"Campo '{col}' não existe na tabela {controller.table_name}")
        
        expected_len = len(columns)
        for idx, row in enumerate(source_data):
            if len(row) != expected_len:
                raise Exception(f"Linha {idx} tem {len(row)} valores, esperado {expected_len}")
        
        placeholders = ', '.join(['?'] * len(columns))
        values_clause = ', '.join([f"({placeholders})" for _ in source_data])
        query = f"INSERT INTO {controller.table_name} ({', '.join(columns)}) VALUES {values_clause}"
        
        flat_values = [val for row in source_data for val in row]
        
        try:
            controller.db.ttsbegin()
            cursor = controller.db.executeCommand(query, tuple(flat_values))
            affected_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else len(source_data)
            controller.db.ttscommit()
            return affected_rows
        except Exception as error:
            controller.db.ttsabort()
            raise Exception(f"Erro ao inserir registros em massa: {error}")

class UpdateManager:
    """
    Gerencia operações UPDATE com validação automática
    """

    @staticmethod
    def validate_update(func: Callable) -> Callable:
        '''Decorator para validar operações de UPDATE'''
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            validate = self.validate_fields()
            if not validate['valid']:
                raise Exception(validate['error'])
            
            if not hasattr(self, 'RECID') or self._get_field_instance('RECID').value is None:
                raise Exception("Atualização sem chave primaria, preencha o campo RECID")
            
            recid_instance = self._get_field_instance('RECID')
            if not self.exists(recid_instance == recid_instance.value):
                raise Exception(f"Registro com RECID {recid_instance.value} não existe na tabela {self.table_name}")
            
            return func(self, *args, **kwargs)
        return wrapper
    
    @validate_update
    def update(controller) -> bool:
        """
        Atualiza um registro existente na tabela
        Returns:
            bool: True se atualizado com sucesso
        """
        recid_instance = controller._get_field_instance('RECID')
        record = controller.select().where(recid_instance == recid_instance.value).limit(1).do_update(False).execute()
        
        values = []
        set_clauses = []
        
        for key in controller.__dict__:
            attr = controller._get_field_instance(key)
            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            if record and (record[0].get(key) == attr.value or record[0].get(key) == getattr(attr, '_value', None)):
                continue
            set_clauses.append(f"{key} = ?")
            values.append(attr.value)
        
        if not values:
            raise Exception("Nenhum campo foi alterado para atualizar.")
        
        query = f"UPDATE {controller.table_name} SET " + ", ".join(set_clauses) + " WHERE RECID = ?"
        values.append(controller._get_field_instance('RECID').value)
        
        try:
            controller.db.ttsbegin()
            controller.db.executeCommand(query, tuple(values))
            controller.db.ttscommit()
            
            recid_instance = controller._get_field_instance('RECID')
            updated_record = controller.select().where(recid_instance == recid_instance.value).limit(1).do_update(False).execute()
            if updated_record:
                controller.set_current(updated_record[0])
            
            return True
        except Exception as error:
            controller.db.ttsabort()
            raise Exception(f"Erro ao atualizar registro: {error}")
    
    def update_recordset(controller, where: Optional[Union[FieldCondition, BinaryExpression]] = None, **fields) -> int:
        """
        Atualiza múltiplos registros em massa
        Args:
            where: Condição WHERE (usando operadores sobrecarregados)
            **fields: Campos a atualizar como kwargs
                Ex: item.update_recordset(where=item.PRICE < 100, ACTIVE=False, PRICE=50)
        Returns:
            int: Número de registros afetados
        """
        validate = controller.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        if not fields:
            raise Exception("Nenhum campo para atualizar")
        
        table_columns = controller.get_table_columns()
        col_names = [col[0] for col in table_columns]
        
        set_values = {}
        for field_key, field_val in fields.items():
            field_name = field_key.upper()
            if field_name not in col_names:
                raise Exception(f"Campo '{field_name}' não existe na tabela {controller.table_name}")
            set_values[field_name] = field_val
        
        set_clauses = [f"{field} = ?" for field in set_values.keys()]
        query = f"UPDATE {controller.table_name} SET " + ", ".join(set_clauses)
        values = list(set_values.values())
        
        if where:
            where_sql, where_values = where.to_sql()
            query += f" WHERE {where_sql}"
            values.extend(where_values if isinstance(where_values, list) else [where_values])
        
        try:
            controller.db.ttsbegin()
            cursor = controller.db.executeCommand(query, tuple(values))
            affected_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            controller.db.ttscommit()
            return affected_rows
        except Exception as error:
            controller.db.ttsabort()
            raise Exception(f"Erro ao atualizar registros em massa: {error}")

class DeleteManager:
    """
    Gerencia operações DELETE com validação automática
    """
    
    @staticmethod
    def validate_delete(func: Callable) -> Callable:
        '''Decorator para validar operações de DELETE'''
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            validate = self.validate_fields()
            if not validate['valid']:
                raise Exception(validate['error'])
            
            if not hasattr(self, 'RECID') or self._get_field_instance('RECID').value is None:
                raise Exception("Exclusão sem chave primaria, preencha o campo RECID")
            
            recid_instance = self._get_field_instance('RECID')
            if not self.exists(recid_instance == recid_instance.value):
                raise Exception(f"Registro com RECID {recid_instance.value} não existe na tabela {self.table_name}")
            
            return func(self, *args, **kwargs)
        return wrapper

    @validate_delete
    def delete(controller) -> bool:
        """
        Exclui um registro da tabela
        Returns:
            bool: True se excluído com sucesso
        """
        query = f"DELETE FROM {controller.table_name} WHERE RECID = ?"
        
        try:
            controller.db.ttsbegin()
            controller.db.executeCommand(query, (controller._get_field_instance('RECID').value,))
            controller.db.ttscommit()
        except Exception as error:
            controller.db.ttsabort()
            raise Exception(f"Erro ao excluir registro: {error}")
        
        controller.clear()
        if hasattr(controller, 'RECID'):
            controller._get_field_instance('RECID').value = None
        
        return True
    
    def delete_from(controller, where: Optional[Union[FieldCondition, BinaryExpression]] = None) -> int:
        """
        Deleta múltiplos registros em massa
        Args:
            where: Condição WHERE (usando operadores sobrecarregados)
        Returns:
            int: Número de registros deletados
        """
        validate = controller.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        query = f"DELETE FROM {controller.table_name}"
        values = []
        
        if where:
            where_sql, where_values = where.to_sql()
            query += f" WHERE {where_sql}"
            values.extend(where_values if isinstance(where_values, list) else [where_values])
        else:
            raise Exception("DELETE sem WHERE não é permitido. Use where=True explicitamente se desejar deletar tudo.")
        
        try:
            controller.db.ttsbegin()
            cursor = controller.db.executeCommand(query, tuple(values))
            affected_rows = cursor.rowcount if hasattr(controller, 'rowcount') else 0
            controller.db.ttscommit()
            return affected_rows
        except Exception as error:
            controller.db.ttsabort()
            raise Exception(f"Erro ao deletar registros em massa: {error}")

class TableController():
    """
    Classe de controle de tabelas do banco de dados (SQL Server) - REFATORADA
    
    Nova API:
    - tabela.select().where(tabela.CAMPO == 5)  # Auto-executa ao iterar
    - tabela.select().where((tabela.CAMPO == 5) & (tabela.OUTRO > 10))
    - tabela.select().join(outra).on(tabela.ID == outra.ID)
    
    Operadores suportados: ==, !=, <, <=, >, >=, in_(), like()
    Operadores lógicos: & (AND), | (OR)
    
    SIMPLICAÇÕES:
    - Sem .c: use tabela.CAMPO diretamente
    - Sem .execute(): auto-executa quando necessário
    - Sem result =: instância é atualizada automaticamente
    - Sem .value: use tabela.CAMPO = valor (setter automático)
    
    Herda de 4 managers:
    - SelectManager: operações SELECT
    - InsertManager: operações INSERT (com decorator @validate_insert)
    - UpdateManager: operações UPDATE (com decorator @validate_update)
    - DeleteManager: operações DELETE (com decorator @validate_delete)
    """
    def __init__(self, db: Union[data, Transaction], table_name: Optional[str] = None):
        '''
        Inicializa o controlador de tabela.
        Args:
            db (Union[data, Transaction]): Instância de conexão ou transação.
            table_name (str): Nome da tabela no banco de dados.
        '''
        #SelectManager.__init__(self, self)
        
        self.db         = db
        self.table_name = (table_name or self.__class__.__name__).upper()

        self.records:     List[Dict[str, Any]]           = []
        self.Columns:     Optional[List[List[Any]]]      = None
        self.Indexes:     Optional[List[str]]            = None
        self.ForeignKeys: Optional[List[Dict[str, Any]]] = None

        self.__select_manager = SelectManager(self)        

    def __getattribute__(self, name: str):
        '''
        Intercepta acesso aos campos para comportamento inteligente:
        - Em contexto de query/operadores: retorna instância EDT/Enum (para operadores)
        - Em contexto normal: retorna o valor diretamente
        '''
        protected_attrs = {
            'db', 'table_name', 'records', 'Columns', 'Indexes', 'ForeignKeys',
            '_where_conditions', '_columns', '_joins', '_order_by', '_limit',
            '_offset', '_group_by', '_having_conditions', '_distinct', '_do_update',
            'controller', '__class__', '__dict__'
        }
        
        if name in protected_attrs or name.startswith('_'):
            return object.__getattribute__(self, name)
        
        attr = object.__getattribute__(self, name)
        
        if callable(attr):
            return attr
        
        if isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
            import inspect
            try:
                frame = inspect.currentframe()
                if frame and frame.f_back:
                    for _ in range(3):
                        if not frame.f_back:
                            break
                        frame = frame.f_back
                        caller_name = frame.f_code.co_name
                        
                        query_contexts = {
                            'where', 'columns', 'order_by', 'group_by', 'join', 'having',
                            '__eq__', '__ne__', '__lt__', '__le__', '__gt__', '__ge__',
                            'in_', 'like', '_extract_value', '_get_field_name',
                            'to_sql', 'execute'
                        }
                        if caller_name in query_contexts:
                            return attr
            except:
                pass
            
            return attr.value
        
        return attr
  
    def __setattr__(self, name: str, value: Any):
        '''Intercepta atribuições para validar EDT/Enum'''
        if name in ('db', 'table_name', 'records', 'Columns', 'Indexes', 'ForeignKeys',
                    '_where_conditions', '_columns', '_joins', '_order_by', '_limit', 
                    '_offset', '_group_by', '_having_conditions', '_distinct', '_do_update',
                    'controller'):
            object.__setattr__(self, name, value)
            return

        if hasattr(self, name):            
            attr = object.__getattribute__(self, name)
            if isinstance(attr, (EDTController, BaseEnumController)):
                if isinstance(value, EDTController):
                    attr.value = value.value
                elif isinstance(value, (BaseEnumController, BaseEnumController.Enum)):
                    if isinstance(value, BaseEnumController):
                        attr.value = value.value
                    else:
                        attr.value = value.value
                else:
                    attr.value = value
                return
        object.__setattr__(self, name, value)    

    def insert(self) -> bool:
        """Insere um novo registro na tabela"""
        return InsertManager.insert(self)
    
    def insert_recordset(self, columns: List[str], source_data: List[tuple]) -> int:
        """Insere múltiplos registros em massa"""
        return InsertManager.insert_recordset(self, columns, source_data)

    def update(self) -> bool:
        """Atualiza um registro existente na tabela"""
        return UpdateManager.update(self)
    
    def update_recordset(self, where: Optional[Union[FieldCondition, BinaryExpression]] = None, **fields) -> int:
        """Atualiza múltiplos registros em massa"""
        return UpdateManager.update_recordset(self, where, **fields)

    def delete(self) -> bool:
        """Exclui um registro da tabela"""
        return DeleteManager.delete(self)
    
    def delete_from(self, where: Optional[Union[FieldCondition, BinaryExpression]] = None) -> int:
        """Deleta múltiplos registros em massa"""
        return DeleteManager.delete_from(self, where)
    
    def select(self) -> "SelectManager":
        return self.__select_manager.__get__(self)

    def _get_field_instance(self, name: str):
        '''
        Retorna a instância EDT/Enum real de um campo (não o valor).
        Use quando precisar acessar métodos do EDT/Enum ou criar queries.
        '''
        return object.__getattribute__(self, name)         

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
        match = re.search(r'\([\s]*([A-Za-z_][A-Za-z0-9_]*|\*|\d+)[\s]*\)', column)
        if match:
            field = match.group(1).upper()
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

    def exists(self, where: Union[FieldCondition, BinaryExpression]) -> bool:
        '''
        Verifica se existem registros que atendem aos critérios especificados.
        Args:
            where: Condição WHERE usando operadores sobrecarregados
                   Ex: tabela.c.RECID == 5
                   Ex: (tabela.c.campo == 5) & (tabela.c.outro > 10)
        Returns:
            bool: True se existir pelo menos um registro, False caso contrário.
        '''
        rows = self.select().where(where).limit(1).do_update(False).execute()
        return len(rows) > 0

    def validate_fields(self) -> Dict[str, Any]:
        '''
        Valida se os campos da instância existem na tabela.
        Returns:
            Dict[str, Any]: {'valid': True/False, 'error': mensagem}
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
        
        if columns != ['*']:
            col_names = [col[0] for col in table_columns]
            for col in columns:
                if self._is_aggregate_function(col):
                    field_name = self._extract_field_from_aggregate(col)
                    if field_name and field_name not in col_names:
                        raise Exception(f"Campo '{field_name}' na agregação '{col}' não existe na tabela")
                elif col not in col_names:
                    raise Exception(f"Coluna inválida: {col}")
        main_alias = self.table_name
        select_columns = []
        if columns == ['*']:
            select_columns += [f"{main_alias}.{col[0]} AS {main_alias}_{col[0]}" for col in table_columns]
        else:
            for col in columns:
                if self._is_aggregate_function(col):
                    alias_name = col.replace('(', '_').replace(')', '').replace('*', 'ALL').replace('.', '_').replace(' ', '')
                    select_columns.append(f"{col} AS {alias_name}")
                else:
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
        
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            group_clauses = [f"{main_alias}.{field}" for field in group_by]
            query += " GROUP BY " + ", ".join(group_clauses)
        
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
        
        if has_aggregates or group_by:
            column_mapping = []  # [(sql_index, field_name, is_aggregate)]
            sql_idx = 0
            
            if columns == ['*']:
                for col in table_columns:
                    column_mapping.append((sql_idx, col[0], False))
                    sql_idx += 1
            else:
                for col in columns:
                    if self._is_aggregate_function(col):
                        field_name = self._extract_field_from_aggregate(col)
                        if field_name:
                            column_mapping.append((sql_idx, field_name, True))
                        else:
                            alias_name = col.replace('(', '_').replace(')', '').replace('*', 'ALL').replace('.', '_').replace(' ', '')
                            column_mapping.append((sql_idx, alias_name, True))
                        sql_idx += 1
                    else:
                        column_mapping.append((sql_idx, col, False))
                        sql_idx += 1
            
            results = []
            for row in rows:
                main_instance = self.__class__(self.db)
                aggregate_extras = {}  # Para COUNT(*) e similares
                
                for sql_idx, field_name, is_agg in column_mapping:
                    value = row[sql_idx]
                    if hasattr(main_instance, field_name):
                        getattr(main_instance, field_name).value = value
                    else:
                        aggregate_extras[field_name] = value
                
                if aggregate_extras:
                    main_instance._aggregate_results = aggregate_extras
                
                results.append(main_instance)
            
            if do_update and results:
                self.records = results
                self.set_current(results[0])
            
            self._query_where = None
            self._query_columns = None
            self._query_options = None
            self._joins = []
            
            return results
        
        if self._joins:
            results = []
            for row in rows:
                idx = 0
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
            
            return result

    def validate_fields(self) -> Dict[str, Any]:
        '''
        Valida se os campos da instância existem na tabela.
        Returns:
            Dict[str, Any]: {'valid': True/False, 'error': mensagem}
        '''
        ret = {'valid': True, 'error': ''}
        instance_fields = [k for k in self.__dict__ if isinstance(self._get_field_instance(k), (EDTController, BaseEnumController, BaseEnumController.Enum))]
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
        instance_fields = {k: self._get_field_instance(k) for k in self.__dict__ if isinstance(self._get_field_instance(k), (EDTController, BaseEnumController, BaseEnumController.Enum))}
        
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
            attr = self._get_field_instance(key)
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
        if isinstance(record, TableController):
            for key in self.__dict__:
                self_attr = self._get_field_instance(key)
                if isinstance(self_attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                    if hasattr(record, key):
                        source_attr = record._get_field_instance(key)
                        if isinstance(source_attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                            self_attr.value = source_attr.value
            return self
        
        for key, value in record.items():
            if hasattr(self, key):
                attr = self._get_field_instance(key)
                if isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                    attr.value = value
                else:
                    setattr(self, key, value)
        return self    

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
