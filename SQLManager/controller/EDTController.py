import re
from typing             import Any, Optional, Dict, Union, TypeAlias
from .SystemController  import SystemController
from ..CoreConfig       import CoreConfig
from .operator import OperationManager

class EDT_Utils:
    '''Classe utilitária para EDTs'''
    def do_test(self, regex_id: str, value: Any) -> bool:
        '''Valida um valor contra um regex_id'''
        if(isinstance(self, EDTController)):
            return self.regex.do_test(regex_id, value)
        elif (isinstance(self, REGEX)):
            return REGEX(regex_id).is_valid(value)
        
    def is_valid(self, value: Any) -> bool:
        '''Verifica se o valor é válido para a instância'''
        if(isinstance(self, EDTController)):
            try:
                self.set_value(value)
                return True
            except ValueError:
                return False  
        elif (isinstance(self, REGEX)):
            if not self._regex_modes:
                return False
            return bool(self._regex_modes.fullmatch(str(value)))

class REGEX (EDT_Utils):
    """Classe REGEX para validações de formatações"""
    _regex_modes: Optional[re.Pattern]

    def __init__(self, regex_id: str):
        self.regexId      = regex_id
        self._regex_modes = self._set_type(regex_id)
    
    def do_test(self, regex_id: str, value: Any) -> bool:
        '''Testa um valor contra um regex_id específico'''
        return REGEX(regex_id).is_valid(value)

    def _set_type(self, regex_id: str) -> Optional[re.Pattern]:
        """
        Define o padrão regex baseado no ID
        Primeiro verifica se existe um regex customizado registrado no CoreConfig,
        depois procura nos padrões built-in
        """
        if CoreConfig.has_regex(regex_id):
            custom_pattern = CoreConfig.get_regex(regex_id)
            return re.compile(custom_pattern) if custom_pattern else None
        
        patterns: Dict[str, str] = {
            "BigInt": r"^\d+n$",
            "bool": r"^[01]$",
            "any": r"^.*$",
            "binary": r"^(1|0)+$",
            "cnpj_cpf": r"^([0-9A-Z]{2}[\.]?[0-9A-Z]{3}[\.]?[0-9A-Z]{3}[\/]?[0-9A-Z]{4}[-]?[0-9]{2})$|^([0-9]{3}[\.]?[0-9]{3}[\.]?[0-9]{3}[-]?[0-9]{2})$",
            "cnpj": r"^([0-9A-Z]{2}[\.]?[0-9A-Z]{3}[\.]?[0-9A-Z]{3}[\/]?[0-9A-Z]{4}[-]?[0-9]{2})$",
            "cpf": r"^([0-9]{3}[\.]?[0-9]{3}[\.]?[0-9]{3}[-]?[0-9]{2})$",
            "cep": r"^\d{5}-?\d{3}$",
            "date": r"^[0-9]{2}[\\\/\-]?[0-9]{2}[\\\/\-]?[0-9]{4}$",
            "datetime": r"^[0-9]{2}[\\\/\-]?[0-9]{2}[\\\/\-]?[0-9]{4}(\s+[0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$",
            "email": r"^[\w\.-]+@([\w-]+\.)+[\w-]{2,4}$",
            "IP": r"^(\d{1,3}\.){3}\d{1,3}$|^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$",
            "ipv4": r"^(\d{1,3}\.){3}\d{1,3}$",
            "ipv6": r"^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$",
            "number": r"^(?:\(?\d{2}\)?\s?)?9?\d{4}-?\d{4}$",
            "onlyLetters": r"^[a-zA-Z\s]+$",
            "onlyNumbers": r"^[0-9]+$",
            "password": r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d]{8,}$",
            "url": r"^(https?:\/\/)?([\w.-]+)\.([a-z]{2,})([\/\w.-]*)*\/?$",
        }
        pattern = patterns.get(regex_id)
        return re.compile(pattern) if pattern else None    

class EDTController(EDT_Utils, OperationManager):
    '''Classe de controle padrão de EDTs'''
    _value: Any
    regex: REGEX
    type_id: Optional[type]
    limit: Optional[int]    

    def __init__(self, regextype: str, type_id: Optional[type] = None, edt_value: Any = None, limit: Optional[int] = None):
        self.regex   = REGEX(regextype)
        self.type_id = type_id
        self._value  = None
        self.limit   = limit

        if edt_value is not None:
            self._value = self.set_value(edt_value, limit)

    def __str__(self) -> str:
        return str(self._value) if self._value is not None else ""

    def __repr__(self) -> str:
        return str(self._value) if self._value is not None else ""

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, val: Any):
        self._value = self.set_value(val)

    @staticmethod
    def any_type() -> Any:
        return Any

    @classmethod
    def create(cls) -> "EDTController":
        return cls("plaintxt")  # ou outro valor padrão

    def set_value(self, edt_value: Any, limit: Optional[int] = None) -> Any:
        if edt_value is None or edt_value == "":
            return edt_value                                
        
        # Valida tipo se definido
        if self.type_id is not None:
            expected_type = self.type_id.value if hasattr(self.type_id, 'value') else self.type_id
            if isinstance(expected_type, type):
                if not isinstance(edt_value, expected_type):
                    raise ValueError(
                        f"\nValor {SystemController.custom_text(edt_value, 'blue')} "
                        f"deve ser do tipo {SystemController.custom_text(expected_type.__name__, 'red', False, True)} "
                        f"e atualmente é {SystemController.custom_text(type(edt_value).__name__, 'red', False, True)}\n"
                    )
        
        # Valida regex (sempre, independente do tipo)
        if not self.regex.is_valid(edt_value):
            raise ValueError(
                f"\nValor {SystemController.custom_text(edt_value, 'blue')} "
                f"não corresponde ao formato esperado.\nFormato esperado: "
                f"{SystemController.custom_text(self.regex.regexId, 'red', False, True)}\n"
            )
        
        # Valida limite se definido
        if limit is not None and len(str(edt_value)) > limit:
            raise ValueError(
                f"\nValor {SystemController.custom_text(edt_value, 'blue')} "
                f"excede o limite de {SystemController.custom_text(limit, 'red', False, True)} caracteres\n"
            )
        
        self._value = edt_value
        return edt_value          

    def value_of(self) -> Any:
        return self._value

    def to_json(self) -> Any:
        return self._value
                
