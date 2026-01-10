---
## Transaction

Transação isolada com conexão própria.

Uma transação totalmente isolada, semelhante a um "copia e cola" da database_connection, mas com conexão própria.

### Exemplo de uso:
	with database.transaction() as trs:
		# Use a transação isolada
		ProductsTable = source.TablePack.ProductsTable(trs)
		# No final, commit ou abort é automático

### Recomendações:
	- Instancie tabelas usando a transaction, não a database_connection.
	- Você pode usar begin, commit, abort normalmente dentro da transação, ou deixar o 'with' cuidar disso automaticamente.
	- Se usar ttsbegin da TableController integrada à tabela, será um nível de tts para a consulta da tabela, não para a transação inteira.

### Importante:
	O commit ou abort é feito automaticamente ao final do bloco 'with'.

---
## database_connection

Classe de controle de banco com pool de conexões e transações.

Foi realizado o processo de modificação para que seja possível usar transações isoladas (KNEX como foi demonstrado).

Porém, todo seu código legado continua funcionando normalmente. Então para necessidades mais "únicas" como UMA tabela que não vai usar níveis de tts pode usar database, SENÃO usar a transaction.

### Modelo de uso:
	database = database_connection()
	database.connect()
	table = source.TablePack.ProductsTable(database)
	table.insert()
	database.disconnect()

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
