from enum import Enum as _Enum, EnumMeta as _EnumMeta

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

class Enum(_Enum, metaclass=CustomEnumMeta):
    ''' Enum customizado'''
    def __init__(self, value, label):
        self._value_ = value
        self.label   = label
    
    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name} ('{self.value}')"
    
    @classmethod
    def get_by_value(cls, val):
        """Obtém um membro do enum pelo seu value"""
        if val is None:
            return None
        if isinstance(val, cls):
            return val
        for member in cls:
            if member.value == val:
                return member
        return None
    
    @classmethod
    def get_by_key(cls, key):
        """Obtém um membro do enum pelo seu name/key"""
        if isinstance(key, str) and hasattr(cls, key):
            return getattr(cls, key)
        return None
    
    @classmethod
    def is_valid(cls, val):
        """Verifica se um valor é válido para este enum"""
        if isinstance(val, cls):
            return True
        if isinstance(val, str) and hasattr(cls, val):
            return True
        return any(member.value == val for member in cls)
    
    @classmethod
    def get_label(cls, val):
        """Obtém o label de um valor"""
        member = cls.get_by_value(val) or cls.get_by_key(val)
        return member.label if member else None
    
    @classmethod
    def get_key(cls, val):
        """Obtém o name/key de um valor"""
        member = cls.get_by_value(val)
        return member.name if member else val if isinstance(val, str) and hasattr(cls, val) else None
    
    @classmethod
    def get_values(cls):
        """Retorna lista de todos os values"""
        return [member.value for member in cls]
    
    @classmethod
    def get_labels(cls):
        """Retorna lista de todos os labels"""
        return [member.label for member in cls]
    
    @classmethod
    def get_keys(cls):
        """Retorna lista de todos os names/keys"""
        return [member.name for member in cls]
    
    @classmethod
    def get_map(cls):
        """Retorna mapa com value e label de todos os membros"""
        return [{'value': member.value, 'label': member.label} for member in cls]

class BaseEnumController:
    '''
    Controlador base para enumerações personalizadas.
    '''   
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
        # Busca por key (name)
        if isinstance(val, str) and hasattr(self.enum_cls, val):
            self._value = getattr(self.enum_cls, val)
            return
        # Busca por value
        for member in self.enum_cls:
            if member.value == val:
                self._value = member
                return
        raise ValueError(f'Valor "{val}" inválido')

    def is_valid(self, val):
        if isinstance(val, self.enum_cls):
            return True
        if isinstance(val, str) and hasattr(self.enum_cls, val):
            return True
        return any(member.value == val for member in self.enum_cls)

    def get_label(self, val):
        if isinstance(val, self.enum_cls):
            return val.label
        if isinstance(val, str) and hasattr(self.enum_cls, val):
            return getattr(self.enum_cls, val).label
        for member in self.enum_cls:
            if member.value == val:
                return member.label
        return None

    def get_key(self, val):
        if isinstance(val, self.enum_cls):
            return val.name
        if isinstance(val, str) and hasattr(self.enum_cls, val):
            return val
        for member in self.enum_cls:
            if member.value == val:
                return member.name
        return None

    def get_values(self):
        return [member.value for member in self.enum_cls]

    def get_labels(self):
        return [member.label for member in self.enum_cls]

    def get_map(self):
        return [{'value': member.value, 'label': member.label} for member in self.enum_cls]

    def get_keys(self):
        return [member.name for member in self.enum_cls]        