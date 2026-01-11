
## Transaction

Transação isolada com conexão própria.

### Exemplo visual: Passo a passo

```python
# 1. Iniciar transação
with database.transaction() as trs:
	# 2. Instanciar tabela usando a transação
	products = source.TablePack.ProductsTable(trs)
	# 3. Inserir registro
	products.NAME = "Produto X"
	products.PRICE = 10.0
	products.insert()
	# 4. Buscar registro
	products.select(where=[{"field": "NAME", "operator": "=", "value": "Produto X"}])
	for prod in products.execute():
		print(prod.NAME, prod.PRICE)
	# 5. Commit ou abort é automático ao sair do bloco
```

### Recomendações:
	- Instancie tabelas usando a transaction, não a database_connection.
	- Você pode usar begin, commit, abort normalmente dentro da transação, ou deixar o 'with' cuidar disso automaticamente.
	- Se usar ttsbegin da TableController integrada à tabela, será um nível de tts para a consulta da tabela, não para a transação inteira.

### Importante:
	O commit ou abort é feito automaticamente ao final do bloco 'with'.


## database_connection

Classe de controle de banco com pool de conexões e transações.

### Exemplo visual: Passo a passo

```python
# 1. Conectar ao banco
database = database_connection()
database.connect()

# 2. Instanciar tabela
products = source.TablePack.ProductsTable(database)

# 3. Inserir registro
products.NAME = "Produto Y"
products.PRICE = 20.0
products.insert()

# 4. Buscar registro
products.select(where=[{"field": "NAME", "operator": "=", "value": "Produto Y"}])
for prod in products.execute():
	print(prod.NAME, prod.PRICE)

# 5. Desconectar
database.disconnect()
```

> Para operações isoladas, utilize a classe `transaction` conforme exemplo acima.


OBS: se for isolado consulte a classe transaction.
Transação isolada, cada uma com sua própria conexão.

Sabe aquele esquema de "copia e cola" da database_connection? Aqui é igual, só que cada transação é realmente separada, sem misturar nada.

Como usar:
	with database.transaction() as trs:
		ProductsTable = source.TablePack.ProductsTable(trs)
		# Pronto, pode usar begin, commit, abort... ou só deixar o 'with' cuidar de tudo.

Recomendo:
	- Se for usar uma tabela, instancia ela com a transaction, não com a database_connection.
	- Pode usar begin, commit, abort dentro da transação, ou só confiar no 'with' pra finalizar.
	- Se usar ttsbegin da TableController junto da tabela, o nível de tts é só pra consulta da tabela, não pra transação inteira.

Importante:
	No final do bloco 'with', o commit ou abort acontece sozinho. Menos dor de cabeça pra você.

# SQLManager - Connection Instructions

Este documento explica como utilizar as classes de conexão e transação do SQLManager, com exemplos práticos e recomendações.

---

## Transaction

Transação isolada com conexão própria, ideal para operações seguras e independentes.

### Exemplo de uso:

```python
with database.transaction() as trs:
    ProductsTable = source.TablePack.ProductsTable(trs)
    # Operações dentro da transação
    ProductsTable.insert()
    # Commit ou abort é automático ao sair do bloco
```

### Recomendações
- Instancie tabelas usando a transaction, não a database_connection.
- Pode usar `begin`, `commit`, `abort` normalmente ou deixar o `with` cuidar disso.
- Se usar `ttsbegin` da TableController, o nível de tts é apenas para a consulta da tabela, não para a transação inteira.

### Importante
O commit ou abort é feito automaticamente ao final do bloco `with`.

---

## database_connection

Classe principal para controle de banco, com pool de conexões e suporte a transações.

### Exemplo de uso:

```python
database = database_connection()
database.connect()
table = source.TablePack.ProductsTable(database)
table.insert()
database.disconnect()
```

> Para operações isoladas, utilize a classe `transaction` conforme exemplo acima.

### Recomendações
- Use `database_connection` para operações simples ou legadas.
- Prefira `transaction` para operações que exigem isolamento ou segurança.

---

## Documentação das Controllers

Para detalhes completos sobre as controllers, métodos e exemplos, consulte:

- [SQLManager/controller/Instructions.md](../controller/Instructions.md)
