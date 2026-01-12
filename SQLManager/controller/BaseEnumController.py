from enum   import Enum as _Enum, EnumMeta as _EnumMeta
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
    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
                
        if bases and any(isinstance(base, CustomEnumMeta) for base in bases):            
            annotations = {}
            for member_name in namespace.get('_member_names_', []):
                annotations[member_name] = cls
                        
            controller_stub_name = f"_{name}Instance"
            controller_stub_attrs = {
                '__module__': cls.__module__,
                '__annotations__': annotations.copy()
            }
                        
            cls._controller_stub = type(controller_stub_name, (BaseEnumController,), controller_stub_attrs)
        
        return cls
    
    def __call__(cls, value=None):
        if isinstance(value, tuple) and len(value) == 2:
            return super().__call__(value)
                
        controller_class_name = f"{cls.__name__}Controller"
                
        attrs = {
            'enum_cls': cls,
            '__module__': cls.__module__
        }
                
        annotations = {}
        for member in cls:
            attrs[member.name] = member
            annotations[member.name] = cls
        
        attrs['__annotations__'] = annotations
                
        controller_cls = type(controller_class_name, (BaseEnumController,), attrs)
                
        controller = object.__new__(controller_cls)
        controller.enum_cls = cls
        
        if value is None:
            controller._value = list(cls)[0]
        else:
            controller._value = None            
            if isinstance(value, cls):
                controller._value = value
            elif isinstance(value, str):
                if hasattr(cls, value):
                    controller._value = getattr(cls, value)
                else:
                    raise ValueError(f'Valor "{value}" inválido para {cls.__name__}')
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
    
    def __getattr__(self, name):
        '''Permite acessar membros do Enum diretamente através do controller'''
        if hasattr(self.enum_cls, name):
            member = getattr(self.enum_cls, name)
            if isinstance(member, _Enum):                
                return self.enum_cls(member)
            return member
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
    def __dir__(self):
        '''Expõe membros do Enum para IntelliSense e autocomplete'''
        base_attrs = set(super().__dir__())
        enum_members = set(member.name for member in self.enum_cls)
        return sorted(base_attrs | enum_members)

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