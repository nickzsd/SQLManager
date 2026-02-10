from typing              import Any, List, Dict, Optional, Union, Callable
from functools           import wraps
import weakref
import inspect
import sys
from concurrent.futures  import ThreadPoolExecutor, as_completed
from threading           import Lock
from ..connection        import database_connection as data, Transaction
from .EDTController      import EDTController
from .BaseEnumController import BaseEnumController

class FieldCondition:
    '''
    Representa uma condição de campo com operador para construção de WHERE clauses
    Também suporta uso em if/while através de __bool__
    '''
    def __init__(self, field_name: str, operator: str, value: Any, table_alias: Optional[str] = None, left_value: Any = None):
        self.field_name = field_name
        self.operator = operator
        self.value = value
        self.table_alias = table_alias
        self.left_value = left_value  # Valor do campo (lado esquerdo da comparação)
    
    def __and__(self, other: 'FieldCondition') -> 'BinaryExpression':
        return BinaryExpression(self, 'AND', other)
    
    def __or__(self, other: 'FieldCondition') -> 'BinaryExpression':
        return BinaryExpression(self, 'OR', other)
    
    def __bool__(self) -> bool:
        '''Permite usar em if/while - executa comparação Python real'''
        if self.left_value is None:
            return True  # Se não temos valor do campo, assume True
        
        left = self.left_value
        right = self.value
        
        if self.operator == '=':
            return left == right
        elif self.operator == '!=':
            return left != right
        elif self.operator == '<':
            return left < right
        elif self.operator == '<=':
            return left <= right
        elif self.operator == '>':
            return left > right
        elif self.operator == '>=':
            return left >= right
        elif self.operator == 'IN':
            return left in right
        elif self.operator == 'LIKE':
            import re
            pattern = str(right).replace('%', '.*').replace('_', '.')
            return bool(re.match(pattern, str(left)))
        return True
    
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

class AutoExecuteWrapper:
    '''Wrapper que delega métodos para SelectManager mas auto-executa quando não há mais encadeamento'''
    
    _pending_executions = []  # Lista de wrappers pendentes de execução
    
    def __init__(self, select_manager):
        self._select_manager = select_manager
        self._executed = False
        self._result_cache = None
        self._finalized = False
        
        # Registra para execução automática no próximo ciclo
        AutoExecuteWrapper._pending_executions.append(weakref.ref(self, self._cleanup_callback))
    
    @staticmethod
    def _cleanup_callback(ref):
        """Callback chamado quando o wrapper é garbage collected"""
        pass
    
    @staticmethod
    def _execute_pending():
        """Executa todos os wrappers pendentes"""
        while AutoExecuteWrapper._pending_executions:
            ref = AutoExecuteWrapper._pending_executions.pop(0)
            wrapper = ref()
            if wrapper and not wrapper._executed:
                try:
                    wrapper._select_manager.execute()
                    wrapper._executed = True
                except:
                    pass
    
    def __del__(self):
        """Auto-executa quando não há mais referência ao wrapper"""
        # DESABILITADO: Execução em __del__ causa problemas com GC durante construção da cadeia
        # Use .execute() explícito ou acesse métodos mágicos (__len__, __bool__, etc)
        pass
    
    def _finalize(self):
        """Finaliza e executa se necessário (chamado apenas em contextos seguros)"""
        if not self._finalized:
            self._finalized = True
            if not self._executed and not self._select_manager._executed:
                try:
                    self._select_manager.execute()
                    self._executed = True
                except:
                    pass
    
    def __getattr__(self, name):
        """Delega todos os métodos para o SelectManager"""
        attr = getattr(self._select_manager, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if result is self._select_manager:
                    return self  # Retorna o próprio wrapper para manter o encadeamento
                return result
            return wrapper
        return attr
    
    def _ensure_executed(self):
        """Garante que a query foi executada e retorna o resultado"""
        if not self._executed:
            self._result_cache = self._select_manager.execute()
            self._executed = True
        return self._result_cache
    
    def execute(self):
        """Executa explicitamente e retorna o controller para acesso aos campos"""
        self._ensure_executed()
        return self._select_manager._controller
    
    def __len__(self):
        """Permite usar len() - auto-executa se necessário"""
        return len(self._ensure_executed())
    
    def __bool__(self):
        """Permite usar em contextos booleanos - auto-executa se necessário"""
        return bool(self._ensure_executed())
    
    def __iter__(self):
        """Permite iterar - auto-executa se necessário"""
        return iter(self._ensure_executed())
    
    def __getitem__(self, index):
        """Permite acesso por índice - auto-executa se necessário"""
        return self._ensure_executed()[index]

class SelectManager:
    '''Gerencia operações SELECT com API fluente - Auto-executa quando a cadeia termina'''
    
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
        self._executed = False        

    @staticmethod
    def _extract_field_name(field: Union[str, EDTController, 'BaseEnumController']) -> str:
        '''Extrai o nome do campo de um EDT/Enum ou retorna a string'''
        if isinstance(field, (EDTController, BaseEnumController)):
            return field._get_field_name()
        # Se vier como string já, retorna direto
        return str(field)

    def __get__(self, instance, owner=None):
        self._controller = instance
        self._executed = False
        return self

    def _should_auto_execute(self):
        """Verifica se deve executar automaticamente baseado no contexto de chamada"""
        try:
            frame = sys._getframe(2)  
            import dis
            code = frame.f_code
            return True
        except:
            return True

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
        '''Adiciona condições WHERE e permite encadeamento'''
        self._where_conditions = condition
        return self    

    def columns(self, *cols: Union[str, EDTController, 'BaseEnumController']) -> 'SelectManager':
        '''Define as colunas a serem retornadas - Aceita campos ou strings'''
        self._columns = [self._extract_field_name(col) for col in cols]
        return self
    
    def join(self, other_table, join_type: str = 'INNER') -> 'JoinBuilder':
        '''Inicia um JOIN com outra tabela'''
        return JoinBuilder(self, other_table, join_type)
    
    def order_by(self, column: Union[str, EDTController, 'BaseEnumController']) -> 'SelectManager':
        '''Define ordenação - Aceita campo ou string'''
        self._order_by = self._extract_field_name(column)
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
        '''Define GROUP BY - Aceita campos ou strings'''
        self._group_by = [self._extract_field_name(col) for col in columns]
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
        if self._executed:
            return self._controller.records if hasattr(self._controller, 'records') else []
        
        self._executed = True
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
        
        if self._do_update:
            if results:
                if self._joins:
                    self._controller.records = [r[0] for r in results] if results and isinstance(results[0], list) else results
                    if results and isinstance(results[0], list):
                        self._controller.set_current(results[0][0])
                    elif results:
                        self._controller.set_current(results[0])
                else:
                    # Resultados simples (dicts)
                    self._controller.records = results
                    if results and isinstance(results[0], dict):
                        self._controller.set_current(results[0])
            else:
                # Sem resultados: limpa os campos e registros
                self._controller.clear()
                self._controller.records = []
                self._controller.records = []
        
        # Limpa o wrapper pendente após execução
        if hasattr(self._controller, '_pending_wrapper'):
            self._controller._pending_wrapper = None
        
        self.records = results

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

    def records(self) -> List[Any]:
        """Retorna os registros obtidos (após execução)"""
        return self._controller.records if hasattr(self._controller, 'records') else []

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

class InsertRecordsetWrapper:
    """Wrapper que permite uso com ou sem .where()"""
    def __init__(self, manager):
        self._manager = manager
        self._result = None
        
    def where(self, key_column: Union[str, EDTController, Any]) -> int:
        """Executa com filtro WHERE"""
        return self._manager.where(key_column)
    
    def __del__(self):
        """Auto-executa se não chamou .where()"""
        if self._result is None and not self._manager._executed:
            try:
                self._result = self._manager._execute_insert()
            except:
                pass  

class InsertRecordsetManager:
    """
    Gerencia operações INSERT em massa com suporte a WHERE (filtro condicional)
    """
    def __init__(self, controller, source_data: Union[List[tuple], List[Dict], List[Any]], columns: Optional[List[str]] = None):
        self._controller = controller
        self._raw_data = source_data
        self._columns = columns
        self._source_data = None
        self._where_condition = None
        self._key_column = None
        self._executed = False
        
        self._process_data()
    
    def _process_data(self):
        """Processa os dados de entrada e extrai colunas e tuplas"""
        if not self._raw_data:
            raise Exception("Dados vazios fornecidos para insert_recordset")
        
        first_item = self._raw_data[0]
        
        if self._columns:
            self._source_data = self._raw_data
            return
        
        if isinstance(first_item, dict): #Dict
            # Primeiro coleta todas as colunas
            all_cols = list(first_item.keys())
            
            # Filtra colunas onde TODOS os valores são None (permite defaults do banco)
            self._columns = []
            for col in all_cols:
                has_value = any(item.get(col) is not None for item in self._raw_data)
                if has_value:
                    self._columns.append(col)
            
            # Cria tuplas apenas com as colunas que têm valores
            self._source_data = []
            for item in self._raw_data:
                row = tuple(item.get(col) for col in self._columns)
                self._source_data.append(row)

        elif hasattr(first_item, '__dataclass_fields__'): #dataclass
            # Primeiro coleta todas as colunas
            all_cols = list(first_item.__dataclass_fields__.keys())
            
            # Filtra colunas onde TODOS os valores são None (permite defaults do banco)
            self._columns = []
            for col in all_cols:
                has_value = any(getattr(item, col, None) is not None for item in self._raw_data)
                if has_value:
                    self._columns.append(col)
            
            # Cria tuplas apenas com as colunas que têm valores
            self._source_data = []
            for item in self._raw_data:
                row = tuple(getattr(item, col) for col in self._columns)
                self._source_data.append(row)

        elif hasattr(first_item, '__dict__'): #Objeto comum

            self._columns = list(first_item.__dict__.keys())
            self._source_data = [tuple(getattr(item, col) for col in self._columns) for item in self._raw_data]
        else:
            raise Exception("Formato de dados não suportado. Use dict, dataclass ou tuplas com colunas definidas")
    
    def where(self, key_column: Union[str, EDTController, Any]) -> int:
        """
        Define a coluna de chave para comparação e executa (insere apenas se não existir)
        Args:
            key_column: Nome da coluna como STRING (ex: 'ITEMID')
        Returns:
            int: Número de registros inseridos
        """        
        # Sempre converter para string
        if isinstance(key_column, str):
            self._key_column = key_column.upper()
        else:
            found = False
            for attr_name in self._controller.__dict__.keys():
                if attr_name.startswith('_'):
                    continue
                try:
                    attr = getattr(self._controller, attr_name)
                    if attr is key_column:
                        self._key_column = attr_name.upper()
                        found = True
                        break
                except:
                    continue
            
            if not found:
                raise Exception(f"Use string no .where(): .where('ITEMID') ao invés de .where(ProductTable.ITEMID)")
        
        if not self._key_column:
            raise Exception("Coluna não foi definida. Use .where('ITEMID')")
            
        if self._key_column not in [col.upper() for col in self._columns]:
            raise Exception(f"Coluna '{self._key_column}' não está na lista de colunas fornecidas: {self._columns}")
                
        self._executed = True
        return self._execute_insert()
    
    def _execute_insert(self) -> int:
        """
        Executa a inserção em massa, filtrando registros existentes se WHERE foi definido
        Returns:
            int: Número de registros inseridos
        """
        validate = self._controller.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        if not self._columns or not self._source_data:
            raise Exception("Colunas e dados são obrigatórios para insert_recordset")
        
        table_columns = self._controller.get_table_columns()
        col_names = [col[0] for col in table_columns]
        
        for col in self._columns:
            if col.upper() not in col_names:
                raise Exception(f"Campo '{col}' não existe na tabela {self._controller.table_name}")
        
        expected_len = len(self._columns)
        for idx, row in enumerate(self._source_data):
            if len(row) != expected_len:
                raise Exception(f"Linha {idx} tem {len(row)} valores, esperado {expected_len}")
        
        try:
            self._controller.db.ttsbegin()
            
            # Se WHERE foi definido, usa CTE com NOT EXISTS
            if self._key_column:
                affected_rows = self._insert_with_not_exists()
            else:
                # Inserção normal sem filtro
                affected_rows = self._insert_all()
            
            self._controller.db.ttscommit()
            return affected_rows
        except Exception as error:
            self._controller.db.ttsabort()
            raise Exception(f"Erro ao inserir registros em massa: {error}")
    
    def _insert_all(self) -> int:
        """Insere todos os registros sem filtro usando bulk insert otimizado"""
        placeholders = ', '.join(['?'] * len(self._columns))
        query = f"INSERT INTO {self._controller.table_name} ({', '.join(self._columns)}) VALUES ({placeholders})"
        
        cursor = self._controller.db.connection.cursor()
        cursor.fast_executemany = True
        cursor.executemany(query, self._source_data)
        total_inserted = cursor.rowcount if hasattr(cursor, 'rowcount') else len(self._source_data)
        cursor.close()
        
        return total_inserted
    
    def _insert_with_not_exists(self) -> int:
        """Insere apenas registros que NÃO existem usando estratégia otimizada em lotes"""
        key_idx = [col.upper() for col in self._columns].index(self._key_column)
        input_keys = [row[key_idx] for row in self._source_data]
        batch_size = 5000  # Aumente conforme o banco suportar
        existing_keys = set()
        for i in range(0, len(input_keys), batch_size):
            batch_keys = input_keys[i:i + batch_size]
            placeholders = ', '.join(['?'] * len(batch_keys))
            check_query = f"SELECT {self._key_column} FROM {self._controller.table_name} WHERE {self._key_column} IN ({placeholders})"
            existing_result = self._controller.db.doQuery(check_query, tuple(batch_keys))
            if existing_result:
                existing_keys.update(row[0] for row in existing_result)
        new_data = [row for row in self._source_data if row[key_idx] not in existing_keys]
        if not new_data:
            return 0
        placeholders = ', '.join(['?'] * len(self._columns))
        query = f"INSERT INTO {self._controller.table_name} ({', '.join(self._columns)}) VALUES ({placeholders})"
        cursor = self._controller.db.connection.cursor()
        cursor.fast_executemany = True
        cursor.executemany(query, new_data)
        total_inserted = cursor.rowcount if hasattr(cursor, 'rowcount') else len(new_data)
        cursor.close()
        return total_inserted
    
    def __await__(self):
        """Permite uso com await se necessário"""
        async def _async_exec():
            return self._execute_insert()
        return _async_exec().__await__()
    
    def __int__(self):
        """Permite conversão direta para int (auto-executa se não executou ainda)"""
        if not self._executed:
            self._executed = True
            return self._execute_insert()
        return 0
    
    def __index__(self):
        """Permite uso em contextos que esperam int"""
        return self.__int__()

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
        # Obter colunas com DEFAULT (usando cache)
        columns_with_default = controller.get_columns_with_defaults()
        
        fields = []
        values = []
        
        for key in controller.__dict__:
            attr = controller._get_field_instance(key)
            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            
            # Pular campos com DEFAULT que estão None (permite DB aplicar default)
            if key in columns_with_default and attr.value is None:
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
    
    def insert_recordset(controller, source_data: Union[List[tuple], List[Dict], List[Any]], columns: Optional[List[str]] = None) -> InsertRecordsetWrapper:
        """
        Insere múltiplos registros em massa (com suporte a WHERE condicional)
        Args:
            source_data: Lista de dicts, dataclasses ou tuplas
            columns: Lista de colunas (opcional, extraído automaticamente de dicts/dataclasses)
        Returns:
            InsertRecordsetWrapper: Use .where() para filtrar, ou deixe auto-executar
        """
        manager = InsertRecordsetManager(controller, source_data, columns)
        return InsertRecordsetWrapper(manager)

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
    def update(controller, _values) -> bool:
        """
        Atualiza um registro existente na tabela
        Returns:
            bool: True se atualizado com sucesso
        """
        recid_instance  = controller._get_field_instance('RECID')
        record          = list(filter(lambda r: r['RECID'] == recid_instance.value, controller.records))
        
        values      = []
        set_clauses = []
        
        for key in controller.__dict__:
            attr = controller._get_field_instance(key)

            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            
            if record:
                old_val = record[0].get(key)
                new_val = _values[0].get(key)
                
                # print(f"Comparando campo {key}: antigo={old_val!r} novo={new_val!r}")
                if old_val == new_val:
                    continue

            set_clauses.append(f"{key} = ?")
            values.append(new_val)
        
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

class AutoExecuteDeleteWrapper:
    '''Wrapper para DeleteRecordsetManager que auto-executa'''
    
    def __init__(self, delete_manager):
        self._delete_manager = delete_manager
        self._executed = False
    
    def __del__(self):
        """Auto-executa quando não há mais referência"""
        if not self._executed:
            try:
                self._delete_manager.execute()
                self._executed = True
            except:
                pass
    
    def execute(self):
        """Executa explicitamente"""
        if not self._executed:
            result = self._delete_manager.execute()
            self._executed = True
            return result
        return self._delete_manager._result_cache
    
    def __int__(self):
        """Permite conversão para int"""
        return self.execute()

class DeleteRecordsetManager:
    '''Gerencia operações DELETE em massa com API fluente - Auto-executa quando a cadeia termina'''
    
    def __init__(self, table_controller):
        self._controller = table_controller
        self._where_conditions: Optional[Union[FieldCondition, BinaryExpression]] = None
        self._executed = False
        self._result_cache = None
    
    def where(self, condition: Union[FieldCondition, BinaryExpression]) -> 'AutoExecuteDeleteWrapper':
        '''Adiciona condições WHERE e retorna wrapper que auto-executa'''
        self._where_conditions = condition
        return AutoExecuteDeleteWrapper(self)
    
    def execute(self) -> int:
        """Executa a operação DELETE e retorna o número de registros deletados"""
        if self._executed:
            return self._result_cache if self._result_cache is not None else 0
        
        self._executed = True
        
        validate = self._controller.validate_fields()
        if not validate['valid']:
            raise Exception(validate['error'])
        
        query = f"DELETE FROM {self._controller.table_name}"
        values = []
        
        if self._where_conditions is None:
            raise Exception("DELETE sem WHERE não é permitido. Use where=True explicitamente se desejar deletar tudo.")
        
        where_sql, where_values = self._where_conditions.to_sql()
        query += f" WHERE {where_sql}"
        values.extend(where_values if isinstance(where_values, list) else [where_values])        
        
        try:
            self._controller.db.ttsbegin()
            cursor = self._controller.db.executeCommand(query, tuple(values))
            affected_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            self._controller.db.ttscommit()
            self._result_cache = affected_rows
            return affected_rows
        except Exception as error:
            self._controller.db.ttsabort()
            raise Exception(f"Erro ao deletar registros em massa: {error}")

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
    
    def delete_from(controller) -> 'DeleteRecordsetManager':
        """
        Deleta múltiplos registros em massa com API fluente
        Uso: table.delete_from().where(table.CAMPO == valor)
        Returns:
            DeleteRecordsetManager: Manager para construir a query de deleção
        """
        return DeleteRecordsetManager(controller)

class TableController():
    """
    Classe de controle de tabelas do banco de dados (SQL Server) - REFATORADA
    
    SELECT:
    - tabela.select().where(tabela.CAMPO == 5)  # Auto-executa!
    - tabela.select().where((tabela.CAMPO == 5) & (tabela.OUTRO > 10))
    - tabela.select().where(tabela.CAMPO == 5).order_by(tabela.NOME)
    - tabela.select().columns(tabela.ID, tabela.NOME).where(tabela.ATIVO == True)
    - tabela.select().where(tabela.ID > 100).limit(10)
    
    INSERT/UPDATE/DELETE em massa:
    - tabela.insert_recordset(['CAMPO1', 'CAMPO2'], [(val1, val2), (val3, val4)]).where('CAMPO1').execute()
    - tabela.update_recordset(where=tabela.CAMPO == 5, NOME='Novo', ATIVO=True)
    - tabela.delete_from().where(tabela.CAMPO < 10)  # Auto-executa!
    
    Operadores suportados: ==, !=, <, <=, >, >=, in_(), like()
    Operadores lógicos: & (AND), | (OR)
    
    IMPORTANTES:
    - USE CAMPOS (tabela.CAMPO) em vez de strings ("CAMPO") nos métodos
    - Sem .execute(): auto-executa quando a linha termina
    - Sem result =: instância é atualizada automaticamente
    - Sem .value: use tabela.CAMPO = valor (setter automático)
    
    Herda de 4 managers:
    - SelectManager: operações SELECT (auto-executa)
    - InsertManager: operações INSERT (com decorator @validate_insert)
    - UpdateManager: operações UPDATE (com decorator @validate_update)
    - DeleteManager: operações DELETE (com decorator @validate_delete)
    """
    
    # Cache estático de colunas com DEFAULT por tabela
    _defaults_cache: Dict[str, set] = {}
    
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

        self.isUpdate = False
        self._pending_wrapper = None  # Rastreia wrapper pendente de execução

        self.__select_manager = SelectManager(self)        

    def __getattribute__(self, name: str):
        '''
        Intercepta acesso aos campos:
        - table.CAMPO retorna o VALOR diretamente (table.CAMPO.value)
        - table.field('CAMPO') retorna o EDT/Enum para construir queries
        - Se houver query pendente, executa antes de retornar o campo
        '''
        protected_attrs = {
            'db', 'table_name', 'records', 'Columns', 'Indexes', 'ForeignKeys',
            '_where_conditions', '_columns', '_joins', '_order_by', '_limit',
            '_offset', '_group_by', '_having_conditions', '_distinct', '_do_update',
            'controller', '__class__', '__dict__', 'isUpdate', '_pending_wrapper',
            '__select_manager', 'field', 'select', 'insert', 'update', 'delete',
            'insert_recordset', 'update_recordset', 'delete_from', 'set_current',
            'clear', 'validate_fields', 'validate_write', 'get_table_columns',
            'get_columns_with_defaults', 'get_table_index', 'get_table_foreign_keys',
            'get_table_total', 'exists', '_get_field_instance', '_is_aggregate_function',
            '_extract_field_from_aggregate', 'SelectForUpdate'
        }
        
        if name in protected_attrs or name.startswith('_'):
            return object.__getattribute__(self, name)
        
        # Se estiver acessando um campo e houver wrapper pendente, executa
        if not name.startswith('_'):
            pending = object.__getattribute__(self, '_pending_wrapper')
            if pending is not None:
                try:
                    pending._finalize()  # Força execução
                    object.__setattr__(self, '_pending_wrapper', None)
                except:
                    pass
        
        attr = object.__getattribute__(self, name)
        
        # Retorna métodos normalmente
        if callable(attr):
            return attr
        
        # Se é EDT/Enum, SEMPRE retorna o VALOR (mesmo que None)
        if isinstance(attr, (EDTController, BaseEnumController)):
            return attr.value if hasattr(attr, 'value') else None
        
        return attr
  
    def __setattr__(self, name: str, value: Any):
        '''Intercepta atribuições para validar EDT/Enum'''
        if name in ('db', 'table_name', 'records', 'Columns', 'Indexes', 'ForeignKeys',
                    '_where_conditions', '_columns', '_joins', '_order_by', '_limit', 
                    '_offset', '_group_by', '_having_conditions', '_distinct', '_do_update',
                    'controller', '_pending_wrapper', '__select_manager'):
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
        
        # Se está criando um novo EDT/Enum, armazena o nome do campo nele
        if isinstance(value, (EDTController, BaseEnumController)):
            value._field_name = name
        
        object.__setattr__(self, name, value)    

    def insert(self) -> bool:
        """Insere um novo registro na tabela"""
        return InsertManager.insert(self)
    
    def insert_recordset(self, source_data: Union[List[tuple], List[Dict], List[Any]], columns: Optional[List[str]] = None) -> InsertRecordsetWrapper:
        """Insere múltiplos registros em massa (auto-executa ou use .where())"""
        return InsertManager.insert_recordset(self, source_data, columns)

    def update(self) -> bool:
        """Atualiza um registro existente na tabela"""
        if(not self.isUpdate):
            raise Exception("Registro não definido para atualização.")
        
        values = [{}]
        for key in self.__dict__:
            attr = self._get_field_instance(key)
            if not (isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum))) or key == 'RECID':
                continue
            values[0][key] = attr.value

        ret = UpdateManager.update(self, values)

        self.isUpdate = False

        return ret  
    
    def SelectForUpdate(self, _update: bool):
        '''Marca o Registro para ser atualizado após um select()
        Uso: table.SelectForUpdate(True) antes de fazer modificações
        '''
        self.isUpdate = _update        

    def update_recordset(self, where: Optional[Union[FieldCondition, BinaryExpression]] = None, **fields) -> int:
        """Atualiza múltiplos registros em massa"""
        return UpdateManager.update_recordset(self, where, **fields)

    def delete(self) -> bool:
        """Exclui um registro da tabela"""
        return DeleteManager.delete(self)
    
    def delete_from(self) -> 'DeleteRecordsetManager':
        """Deleta múltiplos registros em massa com API fluente (auto-executa ou use .where())
        
        Uso:
            # Auto-executa quando termina a linha
            table.delete_from().where(table.CAMPO == valor)
            
            # Ou armazene e execute explicitamente
            result = table.delete_from().where(table.CAMPO == valor).execute()
        
        Returns:
            DeleteRecordsetManager: Manager para construir a query de deleção
        """
        return DeleteManager.delete_from(self)
    
    def select(self) -> "SelectManager":
        manager = self.__select_manager.__get__(self)
        wrapper = AutoExecuteWrapper(manager)
        self._pending_wrapper = wrapper  # Registra wrapper pendente
        return wrapper

    def field(self, name: str):
        '''
        Retorna a instância EDT/Enum real de um campo (para construir queries).
        Use quando precisar construir condições WHERE.
        
        Exemplo:
            table.field('RECID') == 5  # Para queries
            table.RECID            # Para acessar valor
        '''
        return object.__getattribute__(self, name)

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
    
    def get_columns_with_defaults(self) -> set:
        '''
        Retorna conjunto de colunas que possuem DEFAULT definido no banco.
        Usa cache estático para evitar múltiplas queries.
        Returns:
            set: Conjunto com nomes das colunas que têm DEFAULT
        '''
        if self.table_name in TableController._defaults_cache:
            return TableController._defaults_cache[self.table_name]
        
        query = f"""
        SELECT c.name
        FROM sys.columns c
        INNER JOIN sys.tables t ON c.object_id = t.object_id
        WHERE t.name = ? AND c.default_object_id > 0
        """
        defaults_result = self.db.doQuery(query, (self.table_name,))
        columns_with_default = set(row[0] for row in defaults_result) if defaults_result else set()
        
        # Cachear resultado
        TableController._defaults_cache[self.table_name] = columns_with_default
        return columns_with_default

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
        wrapper = self.select().where(where).limit(1).do_update(False)
        rows = wrapper.execute()
        return len(rows) > 0

    def validate_fields(self) -> Dict[str, Any]:
        '''
        Valida se os campos da instância existem na tabela.
        Returns:
            Dict[str, Any]: {'valid': True/False, 'error': mensagem}
        '''
        validate = self.__validate_fields()
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
            column_mapping = [] 
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

    def __validate_fields(self) -> Dict[str, Any]:
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
        Verifica se campos obrigatórios estão preenchidos (exceto os que têm DEFAULT no banco).
        Returns:
            Dict[str, Any]: {'valid': True/False, 'error': mensagem}
        '''
        ret = {'valid': True, 'error': ''}
        columns = self.get_table_columns()
        columns_with_default = self.get_columns_with_defaults()
        
        # Filtrar campos NOT NULL que NÃO têm DEFAULT (esses são realmente obrigatórios)
        required_fields = [
            col[0] for col in columns 
            if col[2] == 'NO' and col[0] != 'RECID' and col[0] not in columns_with_default
        ]
        
        instance_fields = {k: self._get_field_instance(k) for k in self.__dict__ if isinstance(self._get_field_instance(k), (EDTController, BaseEnumController, BaseEnumController.Enum))}
        
        # Validar apenas campos obrigatórios que NÃO têm DEFAULT
        for field in required_fields:
            if field not in instance_fields:
                ret = {'valid': False, 'error': f"Campo obrigatório '{field}' não existe na instância"}
                return ret
            attr = instance_fields[field]
            if attr.value is None or attr.value == '':
                ret = {'valid': False, 'error': f"Campo obrigatório '{field}' não pode ser vazio (campo sem DEFAULT no banco)"}
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
            self: Instância preenchida ou None se record for None
        '''
        if record is None:
            return self
        
        if isinstance(record, TableController):
            for key in self.__dict__:
                self_attr = self._get_field_instance(key)
                if isinstance(self_attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                    if hasattr(record, key):
                        source_attr = record._get_field_instance(key)
                        if isinstance(source_attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                            self_attr.value = source_attr.value
            return self
        
        # Criar mapeamento case-insensitive
        record_upper = {k.upper(): v for k, v in record.items()}
        
        for key in self.__dict__:
            # Pular atributos especiais
            if key.startswith('_') or key in ('db', 'table_name', 'records', 'Columns', 'Indexes', 'ForeignKeys', 'isUpdate'):
                continue
                
            attr = self._get_field_instance(key)
            if isinstance(attr, (EDTController, BaseEnumController, BaseEnumController.Enum)):
                # Busca o valor no dict com case-insensitive
                key_upper = key.upper()
                if key_upper in record_upper:
                    try:
                        attr.value = record_upper[key_upper]
                    except (ValueError, TypeError):
                        # Se falhar ao setar, mantém None
                        pass
        
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
