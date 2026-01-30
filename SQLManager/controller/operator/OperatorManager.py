from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..TableController import FieldCondition


class OperationManager:
    '''
    Mixin que adiciona operadores sobrecarregados para construção de queries
    Permite usar operadores Python (==, !=, <, <=, >, >=) diretamente nos campos
    '''
    
    def _get_field_condition(self):
        '''Import lazy de FieldCondition para evitar importação circular'''
        from ..TableController import FieldCondition
        return FieldCondition
    
    def __eq__(self, other) -> 'FieldCondition':
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        value = self._extract_value(other)
        left_value = self.value if hasattr(self, 'value') else getattr(self, '_value', None)
        return FieldCondition(field_name, '=', value, left_value=left_value)
    
    def __ne__(self, other) -> 'FieldCondition':
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        value = self._extract_value(other)
        left_value = self.value if hasattr(self, 'value') else getattr(self, '_value', None)
        return FieldCondition(field_name, '!=', value, left_value=left_value)
    
    def __lt__(self, other) -> 'FieldCondition':
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        value = self._extract_value(other)
        return FieldCondition(field_name, '<', value)
    
    def __le__(self, other) -> 'FieldCondition':
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        value = self._extract_value(other)
        return FieldCondition(field_name, '<=', value)
    
    def __gt__(self, other) -> 'FieldCondition':
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        value = self._extract_value(other)
        return FieldCondition(field_name, '>', value)
    
    def __ge__(self, other) -> 'FieldCondition':
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        value = self._extract_value(other)
        return FieldCondition(field_name, '>=', value)
    
    def in_(self, values: list) -> 'FieldCondition':
        '''Operador IN para listas de valores'''
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        return FieldCondition(field_name, 'IN', values)
    
    def like(self, pattern: str) -> 'FieldCondition':
        '''Operador LIKE para pattern matching'''
        FieldCondition = self._get_field_condition()
        field_name = self._get_field_name()
        return FieldCondition(field_name, 'LIKE', pattern)
    
    def _extract_value(self, other):
        '''Extrai o valor de EDT, Enum ou retorna o valor direto'''
        from ..EDTController import EDTController
        from ..BaseEnumController import BaseEnumController, Enum
        
        if isinstance(other, EDTController):
            return other.value
        elif isinstance(other, (BaseEnumController, Enum)):
            return other.value if hasattr(other, 'value') else other._value_
        return other
    
    def _get_field_name(self) -> str:
        '''
        Retorna o nome do campo armazenado no EDT/Enum
        '''
        # O nome do campo é injetado pelo TableController.__setattr__
        if hasattr(self, '_field_name'):
            return self._field_name
        
        # Fallback: retorna um nome genérico
        return 'FIELD'
