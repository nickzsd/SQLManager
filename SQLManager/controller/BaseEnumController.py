from enum   import Enum as _Enum, EnumMeta as _EnumMeta
from typing import Union, TypeAlias
from .operator import OperationManager

class BaseEnum_Utils:
    '''Classe utilitária para Enums e Controllers'''
    def _enum_class(self):
        return getattr(self, 'enum_cls', self)

    def get_keys(self):
        return [member.name for member in self._enum_class()]

    def get_values(self):
        return [member.value for member in self._enum_class()]

    def get_labels(self):
        return [member.label for member in self._enum_class()]

    def get_map(self):
        return [{'value': member.value, 'label': member.label} for member in self._enum_class()]

class CustomEnumMeta(_EnumMeta):
    ''' Metaclass customizada '''
    def __call__(cls, value=None):
        if isinstance(value, tuple) and len(value) == 2:
            return super().__call__(value)        
        controller = object.__new__(BaseEnumController)
        controller.enum_cls = cls
        if value is None:
            controller._value = list(cls)[0]
        else:
            controller._value = None            
            if isinstance(value, cls):
                controller._value = value
            elif isinstance(value, str) and hasattr(cls, value):
                controller._value = getattr(cls, value)
            else:            
                for member in cls:
                    if member.value == value:
                        controller._value = member
                        break
                if controller._value is None:
                    raise ValueError(f'Valor "{value}" inválido para {cls.__name__}')
        return controller

class Enum(BaseEnum_Utils, _Enum, metaclass=CustomEnumMeta):
    '''Enum customizado'''
    def __init__(self, value, label):
        self._value_ = value
        self.label   = label
    
    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name} ('{self.value}')"

class BaseEnumController(BaseEnum_Utils, OperationManager):
    '''Controlador base para enumerações personalizadas'''
    _enum_cls      = None
    Enum           = Enum    
    
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, '_enum_cls') and cls._enum_cls is not None:
            for member in cls._enum_cls:
                setattr(cls, member.name, member)
    
    def __init__(self, enum_cls=None, value=None):
        if enum_cls is None:
            enum_cls = self.__class__._enum_cls
        self.enum_cls = enum_cls
        self._value = None

        if value is None:
            self._value = list(enum_cls)[0]
        else:
            self.set_value(value)

    def __str__(self):
        return str(self.value) if self.value is not None else ""

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value})"

    @property
    def value(self):
        return self._value.value if self._value else None
    
    @value.setter
    def value(self, val):
        self.set_value(val)

    @property
    def label(self):
        return self._value.label if self._value else None

    @property
    def key(self):
        return self._value.name if self._value else None

    def set_value(self, val):
        if val is None:
            self._value = None
            return
        if isinstance(val, self.enum_cls):
            self._value = val
            return
        if isinstance(val, str) and hasattr(self.enum_cls, val):
            self._value = getattr(self.enum_cls, val)
            return
        for member in self.enum_cls:
            if member.value == val:
                self._value = member
                return
        raise ValueError(f'Valor "{val}" inválido')
