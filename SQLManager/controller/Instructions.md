# Controllers - SQLManager

Este documento detalha as principais controllers do SQLManager, seus métodos e exemplos de uso. Para referência rápida, consulte este arquivo sempre que precisar entender ou utilizar as controllers do sistema.


## TableController
Classe principal para manipulação de tabelas do banco de dados.

### Exemplo visual: CRUD completo (v2.0)

```python
# 1. Instanciar a tabela
products = TablePack.Products(db)

# 2. Inserir um produto (acesso direto sem .value)
products.NAME = "Produto Teste"
products.PRICE = 99.90
products.insert()  # Insere no banco

# 3. Buscar produto (operadores nativos)
for prod in products.select().where(products.NAME == "Produto Teste"):
	print(prod.NAME, prod.PRICE)  # Acesso direto aos valores

# 4. Atualizar produto
products.NAME = "Produto Atualizado"
products.update()  # Atualiza no banco

# 5. Deletar produto
products.delete()  # Remove do banco
```

### Exemplo visual: JOIN entre tabelas (v2.0)

```python
orders = TablePack.Orders(db)
products = TablePack.Products(db)

# JOIN simples (INNER por padrão)
for order, product in orders.select().join(products).on(orders.PRODUCTID == products.RECID):
	print(order.ORDERID, product.NAME)

# LEFT JOIN (especificando o tipo)
for order, product in orders.select().join(products, 'LEFT').on(orders.PRODUCTID == products.RECID):
	print(order.ORDERID, product.NAME if product.NAME else "Sem produto")

# MÚLTIPLOS JOINs (3 ou mais tabelas)
categories = TablePack.Categories(db)
suppliers = TablePack.Suppliers(db)

for order, product, category, supplier in orders.select()\
	.join(products).on(orders.PRODUCTID == products.RECID)\
	.join(categories, 'LEFT').on(products.CATEGORYID == categories.RECID)\
	.join(suppliers, 'INNER').on(products.SUPPLIERID == suppliers.RECID):
	print(f"Pedido: {order.ORDERID}, Produto: {product.NAME}, Categoria: {category.NAME}, Fornecedor: {supplier.NAME}")

# JOIN com filtros combinados
for order, product in orders.select().join(products).on(orders.PRODUCTID == products.RECID).where((orders.STATUS == 'ACTIVE') & (products.PRICE > 100)):
	print(order.ORDERID, product.NAME, product.PRICE)
```

**Tipos de JOIN disponíveis:**
- `'INNER'` (padrão): Retorna registros com correspondência em ambas tabelas
- `'LEFT'`: Retorna todos da tabela esquerda + correspondências da direita
- `'RIGHT'`: Retorna todos da tabela direita + correspondências da esquerda
- `'FULL'`: Retorna todos os registros de ambas tabelas

**WHERE com campos de tabelas do JOIN:**
```python
# WHERE usando campos da tabela principal E do JOIN
for order, product in orders.select()\
	.join(products).on(orders.PRODUCTID == products.RECID)\
	.where((orders.STATUS == 'ACTIVE') & (products.PRICE > 100)):
	print(f"Pedido {order.ORDERID}: {product.NAME} - R$ {product.PRICE}")

# Funciona com múltiplos JOINs também
categories = TablePack.Categories(db)
for order, product, category in orders.select()\
	.join(products).on(orders.PRODUCTID == products.RECID)\
	.join(categories).on(products.CATEGORYID == categories.RECID)\
	.where((orders.STATUS == 'ACTIVE') & (category.NAME == 'Electronics')):
	print(f"{order.ORDERID} | {product.NAME} | {category.NAME}")
```

**Instâncias dos JOINs são atualizadas automaticamente:**
```python
# Cada tabela do JOIN retorna sua própria instância preenchida
for order, product, category in orders.select()\
	.join(products).on(orders.PRODUCTID == products.RECID)\
	.join(categories).on(products.CATEGORYID == categories.RECID):
	
	# Todas as instâncias estão preenchidas e prontas para uso
	print(f"Order RECID: {order.RECID}")
	print(f"Product RECID: {product.RECID}")
	print(f"Category RECID: {category.RECID}")
	
	# Acesso direto aos valores (sem .value)
	print(f"Nome do produto: {product.NAME}")
	print(f"Preço: {product.PRICE}")
	
	# Pode usar as instâncias para outras operações
	if product.PRICE > 100:
		product.PRICE = product.PRICE * 0.9  # Desconto
		product.update()  # Atualiza no banco
```

**Acessando resultados SEM usar for (direto via execute):**
```python
# Executar e armazenar resultados
results = orders.select()\
	.join(products).on(orders.PRODUCTID == products.RECID)\
	.join(categories).on(products.CATEGORYID == categories.RECID)

# results é uma lista de listas: [[order1, product1, category1], [order2, product2, category2], ...]

# Acessar primeira linha
first_order = results[0][0]
first_product = results[0][1]
first_category = results[0][2]

print(f"Primeiro pedido: {first_order.ORDERID}")
print(f"Produto: {first_product.NAME}")
print(f"Categoria: {first_category.NAME}")

# Separar todas as instâncias por tabela
all_orders = [r[0] for r in results]
all_products = [r[1] for r in results]
all_categories = [r[2] for r in results]

# Agora pode trabalhar com listas separadas
for order in all_orders:
	print(f"Pedido {order.ORDERID} - Status: {order.STATUS}")

for product in all_products:
	if product.PRICE > 100:
		product.PRICE = product.PRICE * 0.9
		product.update()

# Acessar via orders.records (atualizado automaticamente com a tabela principal)
orders.select().join(products).on(orders.PRODUCTID == products.RECID)
print(f"Total de registros: {len(orders.records)}")
for record in orders.records:
	print(f"Order: {record['ORDERID']}")
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

### Modelos INSERT, UPDATE, DELETE

**INSERT - Modelo básico:**
```python
products = TablePack.Products(db)

# Preencher campos (acesso direto, sem .value)
products.NAME = "Notebook Dell"
products.PRICE = 2500.00
products.CATEGORY = "Electronics"
products.ACTIVE = 1

# Inserir no banco
products.insert()
print(f"Produto inserido com RECID: {products.RECID}")
```

**INSERT - Com transação:**
```python
try:
	db.ttsbegin()
	
	products.NAME = "Mouse Gamer"
	products.PRICE = 150.00
	products.insert()
	
	db.ttscommit()
	print("Produto inserido com sucesso!")
except Exception as e:
	db.ttsabort()
	print(f"Erro ao inserir: {e}")
```

**INSERT em massa - insert_recordset:**
```python
# Inserir múltiplos registros de uma vez
columns = ['NAME', 'PRICE', 'CATEGORY', 'ACTIVE']
data = [
	('Teclado Mecânico', 300.00, 'Electronics', 1),
	('Mouse Pad', 50.00, 'Accessories', 1),
	('Headset', 200.00, 'Electronics', 1)
]

affected = products.insert_recordset(columns, data)
print(f"{affected} produtos inseridos")
```

**UPDATE - Modelo básico:**
```python
# Buscar produto
products.select().where(products.RECID == 123)

# Atualizar campos
products.NAME = "Notebook XPS"
products.PRICE = 2800.00

# Salvar no banco
products.update()
print("Produto atualizado!")
```

**UPDATE - Com validação:**
```python
# Buscar e validar
if products.exists(products.RECID == 123):
	products.select().where(products.RECID == 123)
	
	# Atualizar apenas se preço for menor que 3000
	if products.PRICE < 3000:
		products.PRICE = products.PRICE * 1.1  # Aumento de 10%
		products.update()
		print(f"Preço atualizado para: {products.PRICE}")
	else:
		print("Preço já está acima do limite")
else:
	print("Produto não encontrado")
```

**UPDATE em massa - update_recordset:**
```python
# Atualizar múltiplos registros de uma vez (nova sintaxe v2.0)
affected = products.update_recordset(
	where=products.CATEGORY == 'Electronics',
	ACTIVE=0,
	PRICE=0
)
print(f"{affected} produtos desativados")

# Ou com operadores combinados
affected = products.update_recordset(
	where=(products.PRICE < 100) & (products.ACTIVE == 1),
	PRICE=50
)
print(f"{affected} produtos com preço atualizado")
```

**DELETE - Modelo básico:**
```python
# Buscar produto
products.select().where(products.RECID == 123)

# Deletar
products.delete()
print("Produto deletado!")
```

**DELETE - Com confirmação:**
```python
recid_to_delete = 123

# Verificar se existe
if products.exists(products.RECID == recid_to_delete):
	products.select().where(products.RECID == recid_to_delete)
	
	# Confirmar
	print(f"Deletar produto: {products.NAME}?")
	confirm = input("(y/n): ")
	
	if confirm.lower() == 'y':
		products.delete()
		print("Produto deletado!")
	else:
		print("Operação cancelada")
else:
	print("Produto não encontrado")
```

**DELETE em massa - delete_from:**
```python
# Deletar múltiplos registros (nova sintaxe v2.0)
affected = products.delete_from(where=products.ACTIVE == 0)
print(f"{affected} produtos inativos deletados")

# Com operadores combinados
affected = products.delete_from(
	where=(products.PRICE < 10) & (products.CATEGORY == 'Obsolete')
)
print(f"{affected} produtos obsoletos deletados")
```

**Transações complexas (INSERT + UPDATE + DELETE):**
***VERSÃO LEGADO***
```python
try:
	db.ttsbegin()
	
	# INSERT
	new_product = TablePack.Products(db)
	new_product.NAME = "Produto Novo"
	new_product.PRICE = 100.00
	new_product.insert()
	
	# UPDATE
	old_product = TablePack.Products(db)
	old_product.select().where(old_product.RECID == 50)
	old_product.PRICE = old_product.PRICE * 0.9
	old_product.update()
	
	# DELETE
	obsolete = TablePack.Products(db)
	obsolete.select().where(obsolete.RECID == 99)
	obsolete.delete()
	
	db.ttscommit()
	print("Todas as operações concluídas com sucesso!")
	
except Exception as e:
	db.ttsabort()
	print(f"Erro: {e}. Todas as operações foram revertidas.")
```

**Transações complexas (INSERT + UPDATE + DELETE):**
***VERSÃO 2.0***
```python
try:
	with database.transaction() as TRS:
		# ttsbegin() é automático ao entrar no with
		
		# INSERT
		new_product = TablePack.Products(TRS)
		new_product.NAME = "Produto Novo"
		new_product.PRICE = 100.00
		new_product.insert()
		
		# UPDATE
		old_product = TablePack.Products(TRS)
		old_product.select().where(old_product.RECID == 50)
		old_product.PRICE = old_product.PRICE * 0.9
		old_product.update()
		
		# DELETE
		obsolete = TablePack.Products(TRS)
		obsolete.select().where(obsolete.RECID == 99)
		obsolete.delete()
		
		# ttscommit() é automático ao sair com sucesso
		print("Todas as operações concluídas com sucesso!")
		
except Exception as e:
	# ttsabort() é automático em caso de erro
	print(f"Erro: {e}. Todas as operações foram revertidas.")
```

## EDTController
Classe para validação e manipulação de tipos de dados estendidos (EDTs).

### Exemplo visual: Validação automática (v2.0)

```python
from SQLManager import EDTController
from model import EnumPack

# Email válido (acesso direto ao valor)
email = EDTController('email', EnumPack.DataType.String)
email = 'user@empresa.com'  # OK
print(email)  # Imprime: user@empresa.com (via __str__)

# Email inválido
try:
	email = 'invalido'
except ValueError as e:
	print("Erro:", e)

# Limite de caracteres
nome = EDTController('any', EnumPack.DataType.String, limit=10)
nome = 'NomeCurto'  # OK
print(nome)  # Imprime: NomeCurto

try:
	nome = 'NomeMuitoLongo'  # Levanta erro
except ValueError as e:
	print("Erro:", e)

# EDT com regex customizado
produto_code = EDTController('ProductCode', EnumPack.DataType.String)
produto_code = 'PRD-123456'  # (v2.0)

```python
from model.enum import ItemType

# Criando e usando enum (acesso direto ao valor)
tipo = ItemType(ItemType.Service)
print(tipo)  # Imprime: 1 (via __str__)
print(tipo.value)  # 1 (acesso explícito)
print(tipo.label)  # 'Serviço'
print(tipo.key)    # 'Service'

# Criar por valor
tipo2 = ItemType(2)
print(tipo2)  # Imprime: 2
print(tipo2.label)  # 'Produto'

# Criar por nome/key
tipo3 = ItemType('RawMaterial')
print(tipo3)  # Imprime: 3
print(tipo3.label)  # 'Matéria Prima'

# Validação
print(tipo.is_valid(2))  # True
print(tipo.is_valid(999))  # False

# Obter informações
print(tipo.get_label(2))  # 'Produto'
print(tipo.get_key(1))    # 'Service'

# Listar todos
print(tipo.get_values())  # [0, 1, 2, 3]
print(tipo.get_labels())  # ['Nenhum', 'Serviço', 'Produto', 'Matéria Prima']
print(tipo.get_keys())    # ['NoneType', 'Service', 'Product', 'RawMaterial']

# Usar em comparações
if tipo.value == ItemType.Service.value:
	print("É um serviço!")
```

**Principais métodos:**
- `is_valid(val)`: Verifica se o valor é válido para o enum.
- `get_label(val)`: Obtém o label de um valor.
- `get_key(val)`: Obtém o nome/key de um valor.
- `get_values()`: Lista todos os valores do enum.
- `get_labels()`: Lista todos os labels do enum.
- `get_map()`: Mapa value/label de todos os membros.
- `get_keys()`: Lista todos os nomes/keys do enum.
- `__str__()`: Permite imprimir o valor diretamente (sem .value)
print(tipo.label)  # 'Serviço'

# Validação
```
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
