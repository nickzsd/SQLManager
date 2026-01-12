import os
import pyodbc
import threading

from queue import Queue, Empty
from typing import Union, TypeAlias
from ..CoreConfig import CoreConfig

_Connection: TypeAlias = Union['database_connection', 'Transaction']

class _TTS_Manager:
    '''
    Gerenciador de níveis de transação (TTS)
    '''
    
    @staticmethod
    def ttsbegin(self: _Connection):
        '''Adiciona um nível de transação'''
        if self.tts_level == 0:
            self.connection.autocommit = False
        self.tts_level += 1

    @staticmethod
    def ttscommit(self: _Connection):
        '''Remove um nível de transação, e faz commit se for o último'''
        if self.tts_level > 0:
            self.tts_level -= 1
            if self.tts_level == 0:
                self.connection.commit()
                self.connection.autocommit = True

    @staticmethod
    def ttsabort(self: _Connection):
        '''Aborta a transação, desfazendo todas as operações'''
        if self.tts_level > 0:
            self.connection.rollback()
            self.connection.autocommit = True
            self.tts_level = 0

class _Consult_Manager:
    '''
    Gerenciador de consultas (queries) e comandos (execute)
    '''
    
    @staticmethod
    def doQuery(self: _Connection, query: str, params: tuple = ()):
        '''Realiza uma query na conexão'''
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        return results
    
    @staticmethod
    def executeCommand(self: _Connection, command: str, params: tuple = ()):
        '''Executa um comando na conexão'''
        cursor = self.connection.cursor()
        cursor.execute(command, params)
        if isinstance(self, database_connection):
            self.connection.commit()
        cursor.close()

class Transaction (_TTS_Manager, _Consult_Manager):
    """
    Transação isolada com conexão própria.

    Uma transação totalmente isolada, semelhante a um "copia e cola" da database_connection,
    mas com conexão própria.

        with database.transaction() as trs:
            -Use a transação isolada
            ProductsTable = source.TablePack.ProductsTable(trs)
            -No final, commit ou abort é automático

        - Instancie tabelas usando a transaction, não a database_connection.
        - Você pode usar begin, commit, abort normalmente dentro da transação,
          ou deixar o 'with' cuidar disso automaticamente.
        - Se usar ttsbegin da TableController integrada à tabela, será um nível de tts
          para a consulta da tabela, não para a transação inteira.

        O commit ou abort é feito automaticamente ao final do bloco 'with'.
    """
    
    def __init__(self, _dbself: 'database_connection'):
        self._db         = _dbself
        self._connection = None
        self._tts_level  = 0
    
    @property
    def connection(self):
        '''so valida conexão, se não tiver da erro'''
        if not self._connection:
            raise Exception("Use 'with transaction'")
        return self._connection
    
    def __enter__(self):
        '''monta um transação (isolada)'''
        self._connection = self._db._get_connection()
        self._connection.autocommit = True
        self.ttsbegin()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        '''finaliza a transação commit ou abort'''
        try:
            if exc_type:
                self.ttsabort()
            else:
                while self._tts_level > 0:
                    self.ttscommit()
        finally:
            self._db._return_connection(self._connection)
            self._connection = None
        return False

class database_connection (_TTS_Manager, _Consult_Manager):
    '''
    classe de controle de banco com pool de conexões e transações
    Foi realizado o processo de modificação para que seja possivel usar transações isoladas (KNEX como foi demonstrado)

    porem todo seu codigo legado continua funcionando normalmente.
    então para necessidades mais "unicas" como UMA tabela que não vai usar niveis de tts pode usar database
    SENÃO usar a transaction.
    modelo de uso:
        database = database_connection()
        database.connect()
        table = source.TablePack.ProductsTable(database)        
        table.insert()
        database.disconnect()
    
    OBS: se for isolado consulte a classe transaction
    '''
    
    def __init__(self, 
                _Server:    str = None,
                _Database:  str = None,
                _User:      str = None,
                _Password:  str = None,
                _pool_size: int = 5, 
                _timeout:   int = 30):
        """
        Inicializa a conexão com o banco de dados
        
        Args:
            _Server: Servidor do banco (se None, usa CoreConfig ou .env)
            _Database: Database (se None, usa CoreConfig ou .env)
            _User: Usuário (se None, usa CoreConfig ou .env)
            _Password: Senha (se None, usa CoreConfig ou .env)
            _pool_size: Tamanho do pool de conexões
            _timeout: Timeout para conexões
        
        Ordem de prioridade:
            1. Parâmetros passados diretamente
            2. CoreConfig (configurado pelo projeto host)
            3. Variáveis de ambiente (.env)
        """
        if CoreConfig.is_configured():
            config = CoreConfig.get_db_config()
            server = _Server or config['server']
            database = _Database or config['database']
            user = _User or config['user']
            password = _Password or config['password']
            driver = config['driver']
        else:
            server = _Server or os.getenv('DB_SERVER')
            database = _Database or os.getenv('DB_DATABASE')
            user = _User or os.getenv('DB_USER')
            password = _Password or os.getenv('DB_PASSWORD')
            driver = "ODBC Driver 18 for SQL Server"
        
        self.connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=no;TrustServerCertificate=yes;"
        )
        self._pool    = Queue(maxsize=_pool_size)
        self._timeout = _timeout
        self._local   = threading.local() 
    
    @property
    def connection(self):
        if not hasattr(self._local, 'connection') or not self._local.connection:
            self._local.connection = self._get_connection()
        return self._local.connection
    
    @property
    def tts_level(self):
        return getattr(self._local, 'tts_level', 0)
    
    @tts_level.setter
    def tts_level(self, value):
        self._local.tts_level = value
    
    def _get_connection(self):
        '''Pega conexão do pool ou cria nova imediatamente se vazio'''
        try:
            return self._pool.get_nowait()
        except Empty:
            return pyodbc.connect(self.connection_string)
    
    def _return_connection(self, conn):
        '''Devolve conexão ao pool ou fecha se cheio'''
        if not conn:
            return
        try:
            conn.autocommit = True
            self._pool.put_nowait(conn)
        except:
            try:
                conn.close()
            except:
                pass

    def connect(self):
        '''Realiza a conexão atual da thread, ou cria uma nova'''
        return self.connection

    def disconnect(self):
        '''Realiza a desconexão atual da thread, ou fecha a conexão'''
        if hasattr(self._local, 'connection') and self._local.connection:
            if self.tts_level > 0:
                try:
                    self._local.connection.rollback()
                except:
                    pass
                self.tts_level = 0
            self._return_connection(self._local.connection)
            self._local.connection = None
    
    def close_all_connections(self):
        while not self._pool.empty():
            try:
                self._pool.get_nowait().close()
            except:
                break

    def transaction(self) -> Transaction:
        return Transaction(self)
    
