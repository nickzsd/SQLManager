# Controllers - SQLManager

Este documento detalha as principais controllers do SQLManager, seus métodos e exemplos de uso. Para referência rápida, consulte este arquivo sempre que precisar entender ou utilizar as controllers do sistema.


## TableController
Classe principal para manipulação de tabelas do banco de dados.

### Exemplo visual: CRUD completo

```python
# 1. Instanciar a tabela
products = TablePack.ProductsTable(db)

# 2. Inserir um produto
products.RECID = None
products.NAME = "Produto Teste"
products.PRICE = 99.90
products.insert()  # Insere no banco

# 3. Buscar produto
products.select(where=[{"field": "NAME", "operator": "=", "value": "Produto Teste"}])
for prod in products.execute():
	print(prod.NAME, prod.PRICE)

# 4. Atualizar produto
products.NAME = "Produto Atualizado"
products.update()  # Atualiza no banco

# 5. Deletar produto
products.delete()  # Remove do banco
```

### Exemplo visual: JOIN entre tabelas

```python
orders = TablePack.OrdersTable(db)
orders.select().join(products, on=[(orders.PRODUCTID, products.RECID)])
for order, product in orders.execute():
	print(order.ORDERID, product.NAME)
```

**Principais métodos:**
- `insert()`: Insere um novo registro na tabela.
- `update()`: Atualiza um registro existente.
- `delete()`: Exclui um registro da tabela.
- `select(where, columns, options)`: Configura ou executa um SELECT.
- `execute()`: Executa a query SELECT configurada.
- `join(other_table_controller, on, ...)`: Adiciona JOIN dinâmico à query.
- `exists(where, columns)`: Verifica se existem registros que atendem aos critérios.
- `get_table_columns()`: Retorna as colunas da tabela.
- `get_table_index()`: Retorna os índices da tabela.
- `get_table_foreign_keys()`: Retorna as chaves estrangeiras.
- `update_recordset(set_values, where)`: Atualiza múltiplos registros em massa.
- `delete_from(where)`: Deleta múltiplos registros em massa.
- `insert_recordset(columns, source_data)`: Insere múltiplos registros em massa.
- `validate_fields()`: Valida se os campos da instância existem na tabela.
- `validate_write()`: Valida campos obrigatórios antes de inserir/atualizar.
- `clear()`: Limpa os campos da tabela.
- `set_current(record)`: Preenche os campos da tabela com valores do banco.


## EDTController
Classe para validação e manipulação de tipos de dados estendidos (EDTs).

### Exemplo visual: Validação automática

```python
# Email válido
email = EDTController('email', str)
email.value = 'user@empresa.com'  # OK

# Email inválido
try:
	email.value = 'invalido'
except ValueError as e:
	print("Erro:", e)

# Limite de caracteres
nome = EDTController('any', str, limit=10)
nome.value = 'NomeCurto'  # OK
nome.value = 'NomeMuitoLongo'  # Levanta erro
```

**Principais métodos:**
- `is_valid(value)`: Valida se o valor atende ao tipo/regex.
- `set_value(edt_value, limit)`: Define e valida o valor.
- `value`: Propriedade para acessar/alterar o valor.
- `to_json()`: Retorna o valor em formato JSON.


## BaseEnumController & Enum
Sistema para enums customizados com validação e labels.

### Exemplo visual: Enum customizado

```python
from model.enum import ItemType

# Usando enum
tipo = ItemType.Service
print(tipo.value)  # 1
print(tipo.label)  # 'Serviço'

# Validação
print(ItemType.is_valid(2))  # True
print(ItemType.get_label(2))  # 'Produto'
```

**Principais métodos:**
- `is_valid(val)`: Verifica se o valor é válido para o enum.
- `get_label(val)`: Obtém o label de um valor.
- `get_key(val)`: Obtém o nome/key de um valor.
- `get_values()`: Lista todos os valores do enum.
- `get_labels()`: Lista todos os labels do enum.
- `get_map()`: Mapa value/label de todos os membros.
- `get_keys()`: Lista todos os nomes/keys do enum.


## SystemController
Utilitário para funcionalidades do sistema (cores no terminal, logs, validações).

### Exemplo visual: Texto colorido e validação

```python
from SQLManager.controller import SystemController

# Texto colorido no terminal
print(SystemController.custom_text('Atenção!', 'red', is_bold=True))

# Validação de múltiplos EDTs
validations = [edt1, edt2, edt3]
erros = SystemController.validation_check(validations)
if erros:
	print('Erros encontrados:', erros)
```

**Principais métodos:**
- `custom_text(text, color, is_bold=False, is_underline=False)`: Formata texto com cor/estilo.
- `stack_log()`: Exibe stack trace.
- `req_log(req, motivo)`: Log de requisições rejeitadas.
- `validation_check(refvalidations)`: Executa validações e retorna erros.

---

> Para detalhes completos, consulte os arquivos em `SQLManager/controller/`.
