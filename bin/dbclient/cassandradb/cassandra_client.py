from bin import global_settings
from bin.commons.logger import DBLoaderLogger
from bin.dbclient.cassandradb.cassandra_session_manager import CassandraSessionPoolManager
from bin.dbclient.db_client import DbClient

class CassandraClient(DbClient):
    
    def __init__(self):
        self.logger = DBLoaderLogger.get_instance(CassandraClient.__name__)
        self.configuration = global_settings.configuration_reader
        self.cassandra_session_pool_manager = CassandraSessionPoolManager(self.configuration)

    def connect(self, query_manager):
        self.cassandra_session_pool_manager.initialize(query_manager)
        return self.cassandra_session_pool_manager.start()

    def close(self):
        self.cassandra_session_pool_manager.stop()
    
    def get_client_handle(self):
        return self.cassandra_session_pool_manager.get_pool_processing_handle()