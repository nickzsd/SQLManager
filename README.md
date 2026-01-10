# SQLManager - Sistema de Gerenciamento de Banco de Dados

Sistema reutilizável para gerenciamento de conexões de banco de dados, validações de dados (EDTs e BaseEnums) e controle de tabelas.

## Importação do Pacote

Após instalar, use:

```python
from SQLManager import connection, controller, CoreConfig
# ou
from SQLManager.connection import database_connection
from SQLManager.controller import EDTController
```

## Características

- Pool de Conexões: Gerenciamento eficiente de conexões com banco de dados
- Transações Isoladas: Sistema de transações similar ao KNEX.js
- Validações Extensíveis: Sistema de EDTs (Extended Data Types) com regex customizáveis
- BaseEnums: Sistema de enumerações com validação integrada
- Configuração Flexível: Suporte a múltiplos projetos sem modificar o Core
- Type Safety: Validações de tipo e formato em runtime
- Model Generator: Sistema automático de geração de modelos baseado no banco de dados


## Instalação

### Como Repositório Externo

```bash
pip install git+https://github.com/nickzsd/SQLManager.git

# Ou adicione ao requirements.txt
git+https://github.com/nickzsd/SQLManager.git
```

NOTA: O SQLManager será instalado no ambiente virtual (.venv) do seu projeto, não na pasta src/

## Passo Obrigatório: Gerar os Modelos

Após instalar, rode o gerador de modelos para criar as pastas e arquivos necessários:

```bash
python -m SQLManager._model._model_update
```

Esse comando irá criar (ou atualizar) automaticamente as seguintes pastas e arquivos dentro de src/model/:

- src/model/EDTs/    → EDTs customizados (tipos de dados validados)
- src/model/enum/    → Enums customizados (tipos enumerados)
- src/model/tables/  → Classes de tabelas baseadas no banco

> **Importante:**
> - O Enum `DataType` e o EDT `Recid` são obrigatórios e sempre serão gerados automaticamente.
> - O gerador sincroniza os campos das tabelas do banco com os arquivos Python.
> - Não edite manualmente arquivos gerados, exceto para customizações documentadas.

## Exemplos de Arquivos Gerados

### Enum (src/model/enum/ItemType.py)
```python
from SQLManager import BaseEnumController

class ItemType(BaseEnumController.Enum):
    '''
    Enumeração de tipos de item (número/texto), com label descritivo.
    '''
    NoneType    = (0, "Nenhum")    
    Service     = (1, "Serviço")
    Product     = (2, "Produto")
    RawMaterial = (3, "Matéria Prima")
```

### Enum Obrigatório (src/model/enum/DataType.py)
```python
from SQLManager import BaseEnumController

class DataType(BaseEnumController.Enum):
    '''
    Enumeração de tipos de dados (texto/texto), com label descritivo.
    '''
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
```

### EDT (src/model/EDTs/ItemId.py)
```python
from typing import Any
from SQLManager import EDTController
from model.enum import DataType

class ItemId(EDTController):
    '''
    Identificação do item.
    Args:
        value str: Identificação do item
    '''
    def __init__(self, value: Any = ""):
        super().__init__("any", DataType.String, value, 50)
        self.value = value
```

### EDT Obrigatório (src/model/EDTs/Recid.py)
```python
from typing import Any
from SQLManager import EDTController
from model.enum import DataType

class Recid(EDTController):
    '''
    Identificador numérico exclusivo.
    Args:
        value number: Identificador a ser validado
    '''
    def __init__(self, value: Any = 0):
        super().__init__("onlyNumbers", DataType.Number, value)
        self.value = value
```

### Table (src/model/tables/Products.py)
```python
from SQLManager import TableController, EDTController
from model import EDTPack, EnumPack

class Products(TableController):
    '''
    Tabela: Products
    args:
        db_controller: Banco de dados ou transação
    '''
    def __init__(self, db):
        super().__init__(db=db, table_name="Products")
        self.RECID = EDTPack.Recid()
        self.ITEMNAME = EDTController('any', EnumPack.dataType.String, None, 100)
        self.ITEMTYPE = EnumPack.ItemType()
```

### 1. Configure o Core no seu projeto (OBRIGATORIO)

```python
# app.py (na raiz do seu projeto)
import os
import dotenv
from SQLManager import CoreConfig

# Carrega .env do SEU projeto
dotenv.load_dotenv()

# Configurar o Core ANTES de usar
CoreConfig.configure(
    db_server=os.getenv('DB_SERVER'),
    db_database=os.getenv('DB_DATABASE'),
    db_user=os.getenv('DB_USER'),
    db_password=os.getenv('DB_PASSWORD')
)
```

### 2. Registre Regex Customizados (Opcional)

```python
# Registrar validações específicas do seu projeto
CoreConfig.register_multiple_regex({
    'CompanyEmail': r'^[\w\.-]+@minhaempresa\.com\.br$',
    'ProductCode': r'^PRD-\d{6}$',
    'OrderNumber': r'^ORD-\d{8}$'
})
```

### 3. Sistema de Model Generator (Importante)

O Core INCLUI um gerador automatico de modelos (_model_update.py) que:
- Vem junto com o Core quando instalado via pip
- Escaneia as tabelas do banco de dados conectado
- Gera automaticamente classes de modelo na pasta src/model/ do SEU projeto
- Cria estrutura: src/model/EDTs/, src/model/enum/, src/model/tables/
- Atualiza automaticamente __init__.py e importacoes
- Sincroniza campos quando tabelas sao alteradas no banco

**Como usar o _model_update.py:**

```bash
# Após instalar o Core, execute o gerador:
python -m SQLManager._model._model_update

# Ou se preferir:
python .venv/Lib/site-packages/SQLManager/_model/_model_update.py
```

**Requisitos obrigatorios:**
- Seu projeto DEVE ter uma pasta `src/` na raiz
- O gerador criara automaticamente: `src/model/EDTs/`, `src/model/enum/`, `src/model/tables/`
- Todas as tabelas no banco DEVEM ter o campo `RECID` (tipo BIGINT)

**IMPORTANTE - Nomenclatura:**
A coerencia entre nomes de campos no banco e EDTs/Enums e ESTRITAMENTE IMPORTANTE:
- Se tem um EDT chamado `ItemName`, o campo no banco deve se chamar `ITEMNAME`
- Se tem um Enum chamado `ItemType`, o campo no banco deve se chamar `ITEMTYPE` (tipo INT)
- EDTs devem ser do tipo correto no banco (string = varchar/nvarchar, numeros = int/bigint/decimal)
- Campos sem EDT correspondente usarao DataType padrao baseado no tipo SQL

**Exemplo:**
```python
# EDT: src/model/EDTs/ItemName.py
class ItemName(EDTController):
    def __init__(self):
        super().__init__('any', str, limit=100)

# Banco de dados:
CREATE TABLE Products (
    RECID BIGINT PRIMARY KEY,
    ITEMNAME NVARCHAR(100),  -- Sera mapeado para ItemName EDT
    ITEMTYPE INT              -- Se existir Enum ItemType, sera mapeado
)
```

### 2. Registre Regex Customizados (Opcional)

```python
# Registrar validações específicas do seu projeto
CoreConfig.register_multiple_regex({
    'CompanyEmail': r'^[\w\.-]+@minhaempresa\.com\.br$',
    'ProductCode': r'^PRD-\d{6}$',
    'OrderNumber': r'^ORD-\d{8}$'
})
```

## Uso Básico

### Conexão com Banco de Dados

```python

from SQLManager.connection import database_connection

# Conectar (usa configuração do CoreConfig)
db = database_connection()
db.connect()

# Query simples
results = db.doQuery("SELECT * FROM Products WHERE Active = ?", (1,))
for row in results:
    print(row)

# Comando (INSERT/UPDATE/DELETE)
db.executeCommand(
    "INSERT INTO Products (Name, Price) VALUES (?, ?)",
    ('Produto Novo', 99.90)
)

# Desconectar
db.disconnect()
```

### Transações Isoladas

```python
# Transação com commit/rollback automático
with db.transaction() as trs:
    trs.executeCommand(
        "UPDATE Products SET Price = ? WHERE RecId = ?",
        (100.50, 123)
    )
    # Commit automático ao sair do bloco
    # Rollback automático em caso de erro
```

### Transações com Níveis (TTS)

```python
# Níveis de transação (estilo AX/D365)
db.ttsbegin()
try:
    db.executeCommand("UPDATE Table1 SET Field = ?", (value,))
    
    db.ttsbegin()  # Nível 2
    try:
        db.executeCommand("UPDATE Table2 SET Field = ?", (value,))
        db.ttscommit()  # Commit nível 2
    except:
        db.ttsabort()  # Rollback nível 2
    
    db.ttscommit()  # Commit nível 1
except:
    db.ttsabort()  # Rollback tudo
```

### EDTs (Extended Data Types)

```python

from SQLManager.controller import EDTController
from model import EnumPack

# EDT com regex built-in
email = EDTController('email', EnumPack.dataType.String)
email = 'user@example.com'  #  Válido
print(email)  # 'user@example.com'

# EDT com limite de caracteres
name = EDTController('any', EnumPack.dataType.String, limit=50)
name = 'Nome do Produto'  #  Válido

# EDT com regex customizado
product_code = EDTController('ProductCode', EnumPack.dataType.Enum_cls.String)
product_code = 'PRD-123456'  #  Válido

# Validação automática
try:
    email = 'invalid-email'  #  ValueError
except ValueError as e:
    print(f"Erro: {e}")
```

### Regex Built-in Disponíveis

```python
# Documentos
'cnpj'          # 00.000.000/0000-00
'cpf'           # 000.000.000-00
'cnpj_cpf'      # Aceita ambos
'cep'           # 00000-000

# Internet
'email'         # usuario@dominio.com
'url'           # https://exemplo.com
'ipv4'          # 192.168.0.1
'ipv6'          # 2001:0db8:85a3::8a2e:0370:7334

# Básicos
'onlyNumbers'   # Apenas dígitos
'onlyLetters'   # Apenas letras
'date'          # DD/MM/YYYY ou DD-MM-YYYY
'number'        # Telefone brasileiro
'password'      # Mínimo 8 chars, letras e números
```

## Padrões de Uso Avançados

### Criando EDTs Personalizados

```python

from SQLManager.controller import EDTController

class CompanyEmail(EDTController):
    def __init__(self):
        super().__init__(
            regextype='CompanyEmail',
            type_id=str
        )

# Usar
email = CompanyEmail()
email = 'joao@minhaempresa.com.br'
```

### Sistema de Tables (se disponível)

```python
from model import TablePack

# Instanciar tabela
products = TablePack.ProductsTable(db)

# Definir valores
products.RECID = 1
products.NAME  = "Produto Teste"
products.PRICE = 99.90

# Inserir
products.insert()

# Buscar
products.select(recid=1)
print(products.NAME)

# Atualizar
products.NAME = "Produto Atualizado"
products.update()

# Deletar
products.delete()
```

## Estrutura do Projeto Host

```
MeuProjeto/
│
├── .env                   # Suas variáveis de ambiente
├── requirements.txt       # git+https://github.com/nickzsd/SQLManager
├── app.py                 # Configurar CoreConfig aqui
│
├── src/
│   └── model/             # GERADO pelo _model_update.py do Core
│       ├── EDTs/          # EDTs customizados
│       ├── enum/          # Enums customizados
│       └── tables/        # Tables geradas automaticamente
│
└── .venv/                 # Core instalado AQUI via pip
    └── Lib/
        └── site-packages/
            └── core/
                ├── _model/
                │   └── _model_update.py  # Gerador (vem com o Core)
                ├── connection/
                ├── controller/
                └── CoreConfig.py
```

## Configurações Avançadas

### Variáveis de Ambiente (.env)

```env
# Banco de Dados
DB_SERVER=localhost
DB_DATABASE=MeuBanco
DB_USER=admin
DB_PASSWORD=senha123
```

### Configuração Programática

```python
from core import CoreConfig

# Via dicionário
config = {
    'db_server': 'localhost',
    'db_database': 'MeuDB',
    'db_user': 'admin',
    'db_password': 'senha',
    'custom_regex': {
        'CustomPattern': r'^CUSTOM-\d{6}$'
    }
}

CoreConfig.configure_from_dict(config)

# Verificar se configurado
if CoreConfig.is_configured():
    print("Core configurado!")

# Ver configuração atual
config = CoreConfig.get_db_config()
print(config)
```

## Boas Práticas

### 1. Configure uma única vez no início

```python
#  BOM: No main/app.py

from SQLManager import CoreConfig
CoreConfig.configure(load_from_env=True)

# Depois em qualquer lugar

from SQLManager.connection import database_connection
db = database_connection()  # Usa configuração do CoreConfig
```

### 2. Use transações isoladas para operações complexas

```python
#  BOM: Transação isolada
with db.transaction() as trs:
    products.insert(trs)
    inventory.update(trs)
    # Commit/rollback automático

# EVITAR: Múltiplas operações sem transação
db.executeCommand("INSERT ...")
db.executeCommand("UPDATE ...")
```

### 3. Valide dados antes de inserir

```python
#  BOM: Valida antes
email_edt = EDTController('email', str)
# (possui setter automatico mas pode haver um if)
if email_edt.is_valid(user_input):
    email_edt = user_input
else:
    raise ValueError("Email inválido")

#  EVITAR: Inserir sem validar
db.executeCommand("INSERT INTO Users (Email) VALUES (?)", (user_input,))
```

## Troubleshooting

### Erro: "Core não configurado"

```python
# Solução: Configure antes de usar

from SQLManager import CoreConfig
CoreConfig.configure(load_from_env=True)
```

### Erro: "Regex não encontrado"

```python
# Solução: Registre o regex customizado

CoreConfig.register_regex('MeuRegex', r'^PATTERN$')
```

### Erro de conexão com banco

```python
# Verifique a configuração
config = CoreConfig.get_db_config()
print(config)  # Verificar se valores estão corretos

# Teste conexão manual

db = database_connection(
    _Server='localhost',
    _Database='TestDB',
    _User='admin',
    _Password='senha'
)
```

---

**Nota**: Este Core é projetado para ser um repositório independente. Nunca modifique arquivos do Core diretamente no projeto host. Use `CoreConfig` para todas as customizações.
