'''
Atualização automática de modelos (EDTs, Enums e Tables)
Lê banco de dados e sincroniza com aplicação, gerando __init__.py automaticamente
'''

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent

class utils:
    '''Funções utilitárias'''
    
    @staticmethod
    def _clear_init_files_pre_import(_root_dir: Path = None):
        '''Limpa arquivos __init__.py antes de importar'''
        if _root_dir is None:
            _root_dir = ROOT_DIR
        model_path = _root_dir / "src" / "model"
        init_files = [
            model_path / "EDTs" / "__init__.py",
            model_path / "enum" / "__init__.py",
            model_path / "tables" / "__init__.py"
        ]
        
        for init_file in init_files:
            if init_file.exists():
                with open(init_file, 'w', encoding='utf-8') as f:
                    f.write("# Auto-generated file - será atualizado automaticamente\n\n__all__ = []\n")


    @staticmethod
    def stepInfo(_step: str, _desc: str):
        from SQLManager import SystemController
        print(f"\n[{SystemController().custom_text(_step, 'cyan', is_bold=True)}] {_desc}...")

sys.dont_write_bytecode = True
src_dir = ROOT_DIR / "src"
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(ROOT_DIR))

import dotenv
import os
dotenv.load_dotenv()

utils._clear_init_files_pre_import(ROOT_DIR)

from SQLManager import CoreConfig, database_connection, SystemController

if not CoreConfig.is_configured():
    CoreConfig.configure(
        db_server=os.getenv('DB_SERVER'),
        db_database=os.getenv('DB_DATABASE'),
        db_user=os.getenv('DB_USER'),
        db_password=os.getenv('DB_PASSWORD')
    )

def ensure_datatype_enum(enum_path):
    '''Garante que Enum DataType exista'''
    datatype_file = enum_path / "DataType.py"
    datatype_code = '''from SQLManager import BaseEnumController

class DataType(BaseEnumController.Enum):
    """
    Enumeração de tipos de dados (texto/texto), com label descritivo.
    """
    Null      = ("NoneType",  "Tipo de dado Nulo")
    String    = ("str",       "Tipo de dado String")
    Number    = ("int",       "Tipo de dado Number")
    Float     = ("float",     "Tipo de dado Float")
    Boolean   = ("bool",      "Tipo de dado Boolean")
    Array     = ("list",      "Tipo de dado Lista")
    Object    = ("dict",      "Tipo de dado Dicionário")
    Tuple     = ("tuple",     "Tipo de dado Tupla")
    Set       = ("set",       "Tipo de dado Conjunto")
    Bytes     = ("bytes",     "Tipo de dado Bytes")
    Function  = ("function",  "Tipo de dado Função")
    Class     = ("type",      "Tipo de dado Classe")
    Undefined = ("undefined", "Tipo de dado Indefinido")
'''
    with open(datatype_file, 'w', encoding='utf-8') as f:
        f.write(datatype_code)

def ensure_recid_edt(edts_path):
    '''Garante que EDT Recid exista'''
    recid_file = edts_path / "Recid.py"
    recid_code = '''from SQLManager import EDTController
from model.enum import DataType

class Recid(EDTController):
    """
    Identificador numérico exclusivo.
    Args:
        value number: Identificador a ser validado
    """
    def __init__(self, value: EDTController.any_type = 0):
        super().__init__("onlyNumbers", DataType.Number, value)
        self.value = value
'''
    with open(recid_file, 'w', encoding='utf-8') as f:
        f.write(recid_code)

class ModelUpdater:
    '''Atualização automática de modelos'''

    def _generate_model_init(self):
        '''Gera __init__.py da pasta src/model/'''
        model_init_file = self.model_path / "__init__.py"
        content = (
            "from . import EDTs   as EDTPack\n"
            "from . import enum   as EnumPack\n"
            "from . import tables as TablePack\n\n"
            "__all__ = [\n"
            "    \"EDTPack\",\n"
            "    \"EnumPack\",\n"
            "    \"TablePack\",\n"
            "]\n"
        )
        with open(model_init_file, 'w', encoding='utf-8') as f:
            f.write(content)

    def __init__(self):
        self.db = database_connection()
        self.db.connect()
        
        project_root = Path.cwd()
        self.model_path  = project_root / "src" / "model"
        self.edts_path   = self.model_path  / "EDTs"
        self.enums_path  = self.model_path  / "enum"
        self.tables_path = self.model_path  / "tables"

        self.edts_path.mkdir(parents=True, exist_ok=True)
        self.enums_path.mkdir(parents=True, exist_ok=True)
        self.tables_path.mkdir(parents=True, exist_ok=True)
                
        self.available_edts   = {}
        self.available_enums  = {}
        self.available_tables = {}
                
        self.edt_file_to_class   = {}
        self.enum_file_to_class  = {}
        self.table_file_to_class = {}
                
        self.sql_type_mapping = {
            'int':          ('int', 'DataType.Number'),
            'bigint':       ('int', 'DataType.Number'),
            'smallint':     ('int', 'DataType.Number'),
            'tinyint':      ('int', 'DataType.Number'),
            'bit':          ('bool', 'DataType.Boolean'),
            'decimal':      ('float', 'DataType.Float'),
            'numeric':      ('float', 'DataType.Float'),
            'money':        ('float', 'DataType.Float'),
            'float':        ('float', 'DataType.Float'),
            'real':         ('float', 'DataType.Float'),
            'varchar':      ('str', 'DataType.String'),
            'nvarchar':     ('str', 'DataType.String'),
            'char':         ('str', 'DataType.String'),
            'nchar':        ('str', 'DataType.String'),
            'text':         ('str', 'DataType.String'),
            'ntext':        ('str', 'DataType.String'),
            'datetime':     ('str', 'DataType.String'),
            'datetime2':    ('str', 'DataType.String'),
            'date':         ('str', 'DataType.String'),
            'time':         ('str', 'DataType.String'),
        }
    
    def __del__(self):
        '''Fecha conexão ao destruir objeto'''
        if hasattr(self, 'db'):
            self.db.disconnect()
    
    def _clear_init_files(self):
        '''Limpa arquivos __init__.py de EDTs, Enums e Tables'''
        init_files = [
            self.edts_path / "__init__.py",
            self.enums_path / "__init__.py",
            self.tables_path / "__init__.py"
        ]
        
        for init_file in init_files:
            with open(init_file, 'w', encoding='utf-8') as f:
                f.write("# Auto-generated file - será atualizado automaticamente\n\n__all__ = []\n")
        
        print(SystemController().custom_text("Arquivos __init__.py limpos", "red", is_bold=True))
    
    def run(self):
        '''Executa atualização completa'''
        print("="*40)
        print("MODEL UPDATE")
        print("="*40)

        existing_tables = list(self.tables_path.glob("*.py"))
        existing_tables = [f for f in existing_tables if not f.name.startswith("_")]
        
        if existing_tables:
            print(f"\n{SystemController().custom_text('ATENÇÃO', 'red', is_bold=True)}")
            print(f"Tabelas não existentes no banco serão {SystemController().custom_text('REMOVIDAS', 'red', is_bold=True)}.")
            print(f"Faça {SystemController().custom_text('BACKUP', 'yellow', is_bold=True)} de src/model/tables antes de continuar.")
            print(f"\nContinuar? ({SystemController().custom_text('y', 'green')}/{SystemController().custom_text('n', 'red')})")
            resposta = input().strip().lower()
            if resposta != "y":
                print("Cancelado.")
                return

        try:
            utils.stepInfo("00", "Limpando arquivos __init__.py")
            self._clear_init_files()

            utils.stepInfo("00.0", "Gerando __init__.py da pasta src/model/")
            self._generate_model_init()

            utils.stepInfo("00.1", "Garantindo Enum DataType obrigatório")
            ensure_datatype_enum(self.enums_path)

            utils.stepInfo("00.2", "Garantindo EDT Recid obrigatório")
            ensure_recid_edt(self.edts_path)

            utils.stepInfo("01.1", "Escaneando EDTs existentes")
            EDT_Manager._scan_existing_edts(self, _ShowEDTs=True)

            utils.stepInfo("01.2", "Atualizando Model de EDTs")
            EDT_Manager._update_edts_init(self)

            utils.stepInfo("02.1", "Escaneando Enums existentes")
            Enum_Manager._scan_existing_enums(self, _ShowEnums=True)

            utils.stepInfo("02.2", "Atualizando Model de Enums")
            Enum_Manager._update_enums_init(self)

            utils.stepInfo("03.1", "Escaneando Tables existentes")
            Table_Manager._scan_existing_tables(self, _ShowTables=True)

            utils.stepInfo("03.2", "Atualizando Tables")
            Table_Manager._update_tables(self)

            utils.stepInfo("03.3", "Atualizando model de Tables")
            Table_Manager._scan_existing_tables(self, _ShowTables=True)
            Table_Manager._update_tables_init(self)

            print("\n" + "="*40)
            print("ATUALIZAÇÃO CONCLUÍDA COM SUCESSO!")
            print("="*40)

        except Exception as e:
            print(f"\n[ERRO] Falha na atualização: {str(e)}")
            raise

class EDT_Manager:
    '''Gerenciamento de EDTs'''
    
    def _scan_existing_edts(_model: ModelUpdater, _ShowEDTs: bool = False):
        '''Escaneia EDTs existentes no diretório'''
        import re
        
        for file in _model.edts_path.glob("*.py"):
            if file.name.startswith("_") or file.name == "__init__.py":
                continue
                        
            file_name = file.stem
            with open(file, 'r', encoding='utf-8') as f:
                content = f.read()                
                match = re.search(r'^class\s+(\w+)\s*\(', content, re.MULTILINE)
                if match:
                    class_name = match.group(1)
                else:                    
                    class_name = file_name
            
            _model.available_edts[class_name.upper()] = class_name
            _model.edt_file_to_class[file_name] = class_name
        
        print(f"Encontrados: {SystemController().custom_text(len(_model.available_edts), 'red', is_bold=True)} EDTs")

        if(_ShowEDTs):
            print("Lista de EDTs encontrados:")
            for edt in _model.available_edts.values():
                print(f" - {SystemController().custom_text(edt, 'green', is_bold=True)}")
                
    def _update_edts_init(_model: ModelUpdater):
        '''Atualiza __init__.py dos EDTs'''
        init_file = _model.edts_path / "__init__.py"
                
        lines = []
        for file_name, class_name in _model.edt_file_to_class.items():
            lines.append(f"from .{file_name} import {class_name}")

        lines.append("")
        lines.append("__all__ = [")

        for edt in _model.available_edts.values():
            lines.append(f"    \"{edt}\",")

        lines.append("]")                    
        content = "\n".join(lines)
        
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Pacote de EDTs atualizado: {init_file}")

class Enum_Manager:
    '''Gerenciamento de Enums'''

    def _scan_existing_enums(_model: ModelUpdater, _ShowEnums: bool = False):
        '''Escaneia Enums existentes no diretório'''
        import re
        
        for file in _model.enums_path.glob("*.py"):
            if file.name.startswith("_") or file.name == "__init__.py":
                continue
                        
            file_name = file.stem
            with open(file, 'r', encoding='utf-8') as f:
                content = f.read()                                
                match = re.search(r'^class\s+(\w+)\s*\(\s*BaseEnumController\.Enum\s*\)', content, re.MULTILINE)
                if match:
                    class_name = match.group(1)
                else:                    
                    class_name = file_name
            
            _model.available_enums[class_name.upper()] = class_name
            _model.enum_file_to_class[file_name] = class_name
        
        print(f"Encontrados {SystemController().custom_text(len(_model.available_enums), 'red', is_bold=True)} Enums")

        if(_ShowEnums):
            print("Lista de Enums encontrados:")
            for enum in _model.available_enums.values():
                print(f" - {SystemController().custom_text(enum, 'green', is_bold=True)}")

    def _update_enums_init(_model: ModelUpdater):
        '''Atualiza __init__.py dos Enums'''
        init_file = _model.enums_path / "__init__.py"
                
        lines = []
        for file_name, class_name in _model.enum_file_to_class.items():
            lines.append(f"from .{file_name} import {class_name}")

        lines.append("")
        lines.append("__all__ = [")

        for enum in _model.available_enums.values():
            lines.append(f"    \"{enum}\",")

        lines.append("]")                    
        content = "\n".join(lines)
        
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Pacote de Enums atualizado: {init_file}")

class Table_Manager:
    '''Gerenciamento de Tables'''
    
    def _scan_existing_tables(_model: ModelUpdater, _ShowTables: bool = False):
        '''Escaneia Tables existentes no diretório'''
        import re
        
        for file in _model.tables_path.glob("*.py"):
            if file.name.startswith("_") or file.name == "__init__.py":
                continue
                        
            file_name = file.stem
            with open(file, 'r', encoding='utf-8') as f:
                content = f.read()                
                match = re.search(r'^class\s+(\w+)\s*\(', content, re.MULTILINE)
                if match:
                    class_name = match.group(1)
                else:                    
                    class_name = file_name
            
            _model.available_tables[class_name] = file
            _model.table_file_to_class[file_name] = class_name
        
        print(f"Encontrados {SystemController().custom_text(len(_model.available_tables), 'red', is_bold=True)} Tables")

        if(_ShowTables):
            print("Lista de Tables encontrados:")
            for table in _model.available_tables.keys():
                print(f" - {table}")

    def _update_tables(_model: ModelUpdater):
        '''Atualiza Tables baseadas no banco de dados'''        
        query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        
        tables = _model.db.doQuery(query)
        db_tables = [row[0] for row in tables]
        
        print(f"Encontradas {SystemController().custom_text(len(db_tables), 'red', is_bold=True)} tabelas no banco de dados")                        
        
        skipped_tables = []
        for table_name in db_tables:
            error_info = Table_Manager._update_single_table(_model, table_name)
            if error_info:
                skipped_tables.append(error_info)
                
        db_tables_lower = set(t.lower() for t in db_tables)
                
        tables_to_remove = [
            (table_name, file_path)
            for table_name, file_path in _model.available_tables.items()
            if table_name.lower() not in db_tables_lower
        ]

        for table_name, file_path in tables_to_remove:
            print(f"\n[NOTIFICAÇÃO] Tabela '{table_name}' removida da aplicação pois não existe no banco de dados!")
            file_path.unlink()
            if table_name in _model.available_tables:
                del _model.available_tables[table_name]
            file_stem = file_path.stem
            if file_stem in _model.table_file_to_class:
                del _model.table_file_to_class[file_stem]
        
        if skipped_tables:
            print(f"\n{SystemController().custom_text('TABELAS NÃO PROCESSADAS', 'yellow', is_bold=True)}")
            print("="*60)
            for error_info in skipped_tables:
                print(f"{SystemController().custom_text('Tabela:', 'cyan')} {error_info['table']}")
                print(f"{SystemController().custom_text('Motivo:', 'red')} {error_info['reason']}")
                print("-"*60)
        
        Table_Manager._scan_existing_tables(_model, _ShowTables=True)

    def _update_tables_init(_model: ModelUpdater):
        '''Atualiza __init__.py de tables'''
        init_file = _model.tables_path / "__init__.py"
                
        lines = []
        for file_name, class_name in _model.table_file_to_class.items():
            lines.append(f"from .{file_name} import {class_name}")

        lines.append("")
        lines.append("__all__ = [")

        for table in _model.available_tables.keys():
            lines.append(f"    \"{table}\",")

        lines.append("]")                    
        content = "\n".join(lines)
        
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Pacote de Tables atualizado: {init_file}")

    def _update_single_table(_model: ModelUpdater, table_name: str):
        '''
        Atualiza/Cria tabela específica
        Returns: Dict com erro ou None se sucesso
        '''        
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        
        columns = _model.db.doQuery(query, (table_name,))
        
        if not columns:
            return {'table': table_name, 'reason': 'Tabela sem colunas'}
        
        recid_column = None
        for col in columns:
            if col[0].upper() == 'RECID':
                recid_column = col
                break
        
        if not recid_column:
            return {'table': table_name, 'reason': 'Campo RECID obrigatório não encontrado'}
                
        recid_type = recid_column[1].lower()
        if recid_type != 'bigint':
            return {'table': table_name, 'reason': f'Campo RECID deve ser BIGINT (encontrado: {recid_type.upper()})'}
        
        table_file = _model.tables_path / f"{table_name}.py"
        
        try:
            if table_file.exists():
                table_code = Table_Manager._update_existing_table(_model, table_name, columns, table_file)
            else:
                table_code = Table_Manager._generate_table_class(_model, table_name, columns)
                    
            with open(table_file, 'w', encoding='utf-8') as f:
                f.write(table_code)
            
            print(f"Atualizada: {SystemController().custom_text(table_name, 'green', is_bold=True)}")
            return None
        except Exception as e:
            return {'table': table_name, 'reason': f'Erro ao gerar código: {str(e)}'}
    
    def _update_existing_table(_model: ModelUpdater, table_name: str, columns, table_file: Path) -> str:
        '''Atualiza tabela existente preservando métodos customizados'''
        import re
        
        with open(table_file, 'r', encoding='utf-8') as f:
            existing_content = f.read()
        
        existing_fields = {}
        field_pattern = r'self\.(\w+)\s*=\s*(.+)'
        for match in re.finditer(field_pattern, existing_content):
            field_name = match.group(1)
            field_value = match.group(2).strip()
            existing_fields[field_name] = field_value
        
        new_fields = {}
        db_field_names = set()
        for col in columns:
            col_name = col[0].upper()
            sql_type = col[1].lower()
            max_length = col[3]
            db_field_names.add(col_name)
            
            if col_name in existing_fields:
                existing_def = existing_fields[col_name]
                if 'EDTPack.' in existing_def or 'EnumPack.' in existing_def and 'Enum_cls' not in existing_def:
                    new_fields[col_name] = existing_def
                else:
                    new_fields[col_name] = Table_Manager._detect_field_type(_model, col_name, sql_type, max_length)
            else:
                new_fields[col_name] = Table_Manager._detect_field_type(_model, col_name, sql_type, max_length)
        
        existing_field_names = set(existing_fields.keys())
        
        new_field_names = db_field_names - existing_field_names
        if new_field_names:
            print(f"  - Tabela {SystemController().custom_text(table_name, 'cyan')}: {SystemController().custom_text('Campos adicionados', 'yellow')} - {', '.join(sorted(new_field_names))}")
        
        removed_field_names = existing_field_names - db_field_names
        if removed_field_names:
            print(f"  - Tabela {SystemController().custom_text(table_name, 'cyan')}: {SystemController().custom_text('Campos removidos do banco', 'red')} - {', '.join(sorted(removed_field_names))}")
        
        init_end_pattern = r'(self\.\w+\s*=\s*.+?)(\n\n|\n    def |\nclass |\Z)'
        matches = list(re.finditer(init_end_pattern, existing_content, re.DOTALL))
        
        custom_methods = ""
        if matches:
            last_field_end = matches[-1].end(1)
            rest_of_file = existing_content[last_field_end:]
            custom_pattern = r'(\n    def (?!__init__)\w+.+?)(?=\nclass |\Z)'
            custom_match = re.search(custom_pattern, rest_of_file, re.DOTALL)
            if custom_match:
                custom_methods = custom_match.group(1)
        
        lines = []
        lines.append("from SQLManager import TableController, EDTController")
        lines.append("from model import EDTPack, EnumPack")
        lines.append("")
        lines.append(f"class {table_name}(TableController):")
        lines.append("    ")
        lines.append("    '''")
        lines.append(f"    Tabela: {table_name}")
        lines.append("    args:")
        lines.append("        db_controller: Banco de dados ou transação")
        lines.append("    '''")
        lines.append("    def __init__(self, db):")
        lines.append(f"        super().__init__(db=db, table_name=\"{table_name}\")")
        lines.append("    ")
        
        for col in columns:
            col_name = col[0].upper()
            if col_name in new_fields:
                lines.append(f"        self.{col_name} = {new_fields[col_name]}")
        
        lines.append("")
        
        if custom_methods:
            lines.append(custom_methods.rstrip())
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_table_class(_model: ModelUpdater, table_name: str, columns) -> str:
        '''Gera código Python para classe de tabela'''
        lines = []
                
        lines.append("from SQLManager import TableController, EDTController")
        lines.append("from model import EDTPack, EnumPack")        
        lines.append("")
                
        lines.append(f"class {table_name}(TableController):")
        lines.append("    ")
        lines.append("    '''")
        lines.append(f"    Tabela: {table_name}")
        lines.append("    args:")
        lines.append("        db_controller: Banco de dados ou transação")
        lines.append("    '''")
        lines.append("    def __init__(self, db):")
        lines.append(f"        super().__init__(db=db, table_name=\"{table_name}\")")
        lines.append("    ")
                
        for col in columns:
            col_name = col[0].upper()
            sql_type = col[1].lower()
            max_length = col[3]
                        
            field_def = Table_Manager._detect_field_type(_model, col_name, sql_type, max_length)
            lines.append(f"        self.{col_name} = {field_def}")
        
        lines.append("")
        
        return "\n".join(lines)
        
    def _detect_field_type(_model: ModelUpdater, field_name: str, sql_type: str, max_length) -> str:
        '''Detecta tipo de campo apropriado (EDT, Enum ou padrão)'''
                
        if field_name in _model.available_edts:
            return f"EDTPack.{_model.available_edts[field_name]}()"
                    
        if field_name in _model.available_enums:
            return f"EnumPack.{_model.available_enums[field_name]}()"
                
        python_type, datatype = _model.sql_type_mapping.get(sql_type, ('str', 'DataType.String'))
                
        datatype_name = datatype.split('.')[-1]

        if sql_type in ('varchar', 'nvarchar', 'char', 'nchar') and max_length:
            return f"EDTController('any', EnumPack.DataType.{datatype_name}, None, {max_length})"
    
        return f"EDTController('any', EnumPack.DataType.{datatype_name})"
    
if __name__ == "__main__":
    updater = ModelUpdater().run()    
