# SQLManager 2.0 - Patch Notes

Data: 12 de Janeiro de 2026
Versão: 2.0.0

---

## BREAKING CHANGES

### 1. TableController - Refatoração Completa com API Fluente

ANTES (v1.x):
```python
# Sintaxe verbosa com dicionários
products.select(
    where=[{'field': 'PRICE', 'operator': '>', 'value': 100}],
    columns=['*'],
    options={'limit': 10, 'orderBy': 'NAME'}
)

# Acesso sempre via .value
nome = products.NAME.value
products.NAME.value = "Novo Nome"

# JOIN complexo com tuplas
products.select().join(
    categories,
    on=[(products.CATEGORYID, categories.RECID)],
    join_type='INNER'
)
```

DEPOIS (v2.0):
```python
# consultas mais claras e naturais
products.select().where(products.PRICE > 100).order_by(products.NAME).limit(10)

# Acesso direto ao valor (mais natural)
nome = products.NAME  # Retorna string diretamente
products.NAME = "Novo Nome"  # Setter automático

# Operadores sobrecarregados nativos
products.select().where((products.PRICE > 100) & (products.ACTIVE == 1))

# JOIN simplificado
products.select().join(categories).on(products.CATEGORYID == categories.RECID)
```

**Motivo:** Simplificar a sintaxe e tornar o código mais legível, aproximando da API do SQLAlchemy/Django ORM.

---

### 2. BaseEnumController - Herança Múltipla Corrigida

ANTES (v1.x):
```python
# Ordem incorreta (causava erro no Python 3.13+)
class Enum(_Enum, BaseEnum_Utils, metaclass=CustomEnumMeta):
    pass
```

DEPOIS (v2.0):
```python
# Ordem corrigida para Python 3.13+
class Enum(BaseEnum_Utils, _Enum, metaclass=CustomEnumMeta):
    pass
```

**Motivo:** Compatibilidade com Python 3.13+ que exige ordem específica em herança múltipla com Enum.

---

### 3. EDTController - Validação de Tipo Corrigida

ANTES (v1.x):
```python
# Comparava strings de tipos
expected_type = str(self.type_id)
if type(edt_value).__name__ != expected_type:
    raise ValueError(...)
```

DEPOIS (v2.0):
```python
# Usa isinstance() corretamente
if self.type_id is not None:
    expected_type = self.type_id.value if hasattr(self.type_id, 'value') else self.type_id
    if not isinstance(edt_value, expected_type):
        raise ValueError(...)
```

**Motivo:** Validação mais robusta e correta de tipos Python.

---

## NOVAS FUNCIONALIDADES

### 1. OperationManager - Operadores Sobrecarregados

NOVO (v2.0):
```python
# Operadores nativos do Python
tabela.select().where(tabela.CAMPO == 5)
tabela.select().where(tabela.CAMPO != 5)
tabela.select().where(tabela.CAMPO < 5)
tabela.select().where(tabela.CAMPO <= 5)
tabela.select().where(tabela.CAMPO > 5)
tabela.select().where(tabela.CAMPO >= 5)
tabela.select().where(tabela.CAMPO.in_([1, 2, 3]))
tabela.select().where(tabela.CAMPO.like('%texto%'))

# Operadores lógicos
tabela.select().where((tabela.CAMPO1 == 5) & (tabela.CAMPO2 > 10))  # AND
tabela.select().where((tabela.CAMPO1 == 5) | (tabela.CAMPO2 > 10))  # OR
```

**Benefício:** Sintaxe natural e intuitiva para construção de queries.

---

### 2. TableController - Acesso Contextual Inteligente

NOVO (v2.0):
```python
# Contexto de query: retorna instância para operadores
query = products.select().where(products.NOME == 'Teste')

# Contexto normal: retorna valor direto
nome = products.NOME  # Retorna string
print(f"Nome: {nome}")  # Não precisa .value

# Ainda pode acessar instância se precisar
nome_instance = products._get_field_instance('NOME')
```

**Benefício:** Menos verbosidade no código, comportamento inteligente baseado no contexto.

---

### 3. Decorators de Validação Automática

NOVO (v2.0):
```python
# Validações aplicadas automaticamente via decorators
@validate_insert
def insert(self):
    # Valida campos e obrigatoriedade automaticamente
    pass

@validate_update
def update(self):
    # Valida RECID e existência automaticamente
    pass

@validate_delete
def delete(self):
    # Valida RECID e existência automaticamente
    pass
```

**Benefício:** Código mais limpo e validações consistentes.

---

### 4. Manager Pattern na TableController

NOVO (v2.0):
```python
# TableController agora herda de 4 managers especializados
class TableController(SelectManager, InsertManager, UpdateManager, DeleteManager):
    pass

# SelectManager: operações SELECT
# InsertManager: operações INSERT (com @validate_insert)
# UpdateManager: operações UPDATE (com @validate_update)  
# DeleteManager: operações DELETE (com @validate_delete)
```

**Benefício:** Separação de responsabilidades e código mais organizado.

---

## MELHORIAS

### 1. database_connection - Refatoração de Managers

ANTES (v1.x):
```python
# Métodos espalhados pela classe
class database_connection:
    def doQuery(self, query, params):
        pass
    
    def executeCommand(self, command, params):
        pass
    
    def ttsbegin(self):
        pass
    
    def ttscommit(self):
        pass
    
    def ttsabort(self):
        pass
```

DEPOIS (v2.0):
```python
# Managers especializados com mixins
class _TTS_Manager:
    @staticmethod
    def ttsbegin(self): pass
    
    @staticmethod
    def ttscommit(self): pass
    
    @staticmethod
    def ttsabort(self): pass

class _Consult_Manager:
    @staticmethod
    def doQuery(self, query, params): pass
    
    @staticmethod
    def executeCommand(self, command, params): pass

class database_connection(_TTS_Manager, _Consult_Manager):
    pass

class Transaction(_TTS_Manager, _Consult_Manager):
    pass
```

**Benefício:** Reutilização de código entre database_connection e Transaction.

---

### 2. _model_update - Tratamento de Erros Não-Bloqueante

ANTES (v1.x):
```python
# Parava na primeira tabela com erro
if not recid_column:
    raise Exception(f"Tabela {table_name} não possui RECID")
```

DEPOIS (v2.0):
```python
# Coleta todos os erros e continua processamento
skipped_tables = []
for table_name in db_tables:
    error_info = _update_single_table(table_name)
    if error_info:
        skipped_tables.append(error_info)

# Exibe relatório completo no final
if skipped_tables:
    print("TABELAS NÃO PROCESSADAS")
    for error_info in skipped_tables:
        print(f"Tabela: {error_info['table']}")
        print(f"Motivo: {error_info['reason']}")
```

**Benefício:** Processa o máximo de tabelas possível e fornece visibilidade completa dos erros.

---

### 3. _model_update - Mensagem de Segurança Condicional

ANTES (v1.x):
```python
# Sempre exibia aviso, mesmo quando não havia tabelas
print("ATENÇÃO! Esta execução pode APAGAR arquivos...")
resposta = input().strip().lower()
```

DEPOIS (v2.0):
```python
# Só exibe se realmente houver tabelas para remover
existing_tables = list(tables_path.glob("*.py"))
if existing_tables:
    print(f"{SystemController().custom_text('ATENÇÃO', 'red', is_bold=True)}")
    print(f"Tabelas não existentes no banco serão {SystemController().custom_text('REMOVIDAS', 'red', is_bold=True)}.")
    resposta = input().strip().lower()
```

**Benefício:** Mensagem mais contextual e menos intrusiva.

---

### 4. CoreConfig - Docstrings Raw para Regex

ANTES (v1.x):
```python
"""
Exemplo:
    CoreConfig.register_regex('Email', r'^[\w\.-]+@company\.com$')
"""
# Gerava SyntaxWarning: invalid escape sequence
```

DEPOIS (v2.0):
```python
r"""
Exemplo:
    CoreConfig.register_regex('Email', r'^[\w\.-]+@company\.com$')
"""
# Sem warnings
```

**Benefício:** Eliminação de warnings de escape inválido no Python.

---

## LIMPEZA DE CÓDIGO

### 1. Remoção de Comentários Excessivos

ANTES (v1.x):
```python
# Configurações de banco de dados
_db_server: Optional[str] = None
# Registro de regex customizados
_custom_regex: Dict[str, str] = {}
# Flag para verificar se foi configurado
_is_configured: bool = False
# Verifica se é um regex customizado
if CoreConfig.has_regex(regex_id):
    # Padrões built-in do Core
    patterns: Dict[str, str] = {
```

DEPOIS (v2.0):
```python
_db_server: Optional[str] = None
_custom_regex: Dict[str, str] = {}
_is_configured: bool = False
if CoreConfig.has_regex(regex_id):
    patterns: Dict[str, str] = {
```

**Benefício:** Código mais limpo e legível.

---

### 2. Docstrings Simplificadas

ANTES (v1.x):
```python
"""
Classe auxiliar para gerenciamento de EDTs
- Parametro:
- use _model: Instancia do ModelUpdater
- Use _ShowEDTs para exibir a lista completa
"""
```

DEPOIS (v2.0):
```python
'''Gerenciamento de EDTs'''
```

**Benefício:** Documentação mais concisa e direta.

---

## ESTRUTURA DO PROJETO

### Novos Arquivos Criados

1. `SQLManager/controller/operator/OperatorManager.py` - Gerenciador de operadores sobrecarregados
2. `SQLManager/controller/operator/__init__.py` - Exporta OperationManager
3. `SQLManager/documents/PatchNote_2.0.md` - Este arquivo

### Arquivos Modificados

1. `SQLManager/CoreConfig.py` - Docstrings raw, remoção de comentários
2. `SQLManager/_model/_model_update.py` - Erro não-bloqueante, warning condicional
3. `SQLManager/connection/database_connection.py` - Refatoração com managers
4. `SQLManager/controller/BaseEnumController.py` - Herança corrigida, utils extraídos
5. `SQLManager/controller/EDTController.py` - Validação corrigida, utils extraídos
6. `SQLManager/controller/TableController.py` - Refatoração completa com API fluente
7. `README.md` - Atualização com novos exemplos
8. `SQLManager/connection/Instructions.md` - Atualização com novos exemplos
9. `setup.py` - (sem mudanças significativas)

---

## COMPATIBILIDADE

### Python
- **Mínimo:** Python 3.8+
- **Testado:** Python 3.13
- **Recomendado:** Python 3.11+

### Breaking Changes
- TableController: `.select()` agora retorna SelectManager ao invés de executar imediatamente
- TableController: Acesso a campos retorna valor direto ao invés de instância (use `._get_field_instance()` se precisar)
- TableController: WHERE agora usa operadores ao invés de dicionários
- TableController: JOIN agora usa `.on()` ao invés de parâmetro direto

---

## MIGRAÇÃO v1.x → v2.0

### Queries SELECT

```python
# v1.x
products.select(
    where=[{'field': 'PRICE', 'operator': '>', 'value': 100}],
    columns=['NAME', 'PRICE'],
    options={'limit': 10}
)

# v2.0
products.select().where(products.PRICE > 100).columns(products.NAME, products.PRICE).limit(10)
```

### Acesso a Campos

```python
# v1.x
nome = products.NAME.value
products.NAME.value = "Novo"

# v2.0
nome = products.NAME  # Direto
products.NAME = "Novo"  # Setter automático

# Se precisar da instância (raro)
nome_instance = products._get_field_instance('NAME')
```

### Operações em Massa

```python
# v1.x (sem mudanças)
products.update_recordset(
    set_values={'PRICE': 100},
    where=[{'field': 'CATEGORY', 'operator': '=', 'value': 'Electronics'}]
)

# v2.0 (nova sintaxe opcional)
products.update_recordset(
    where=products.CATEGORY == 'Electronics',
    PRICE=100
)
```

---

## AGRADECIMENTOS

Agradecimentos especiais aos testes e feedback que possibilitaram esta versão.

---

## SUPORTE

Para problemas ou dúvidas:
- Documentação: [README.md](../../README.md)
- Instructions: [controller/Instructions.md](../controller/Instructions.md)

---

**IMPORTANTE:** Esta versão contém breaking changes. Revise seu código.
