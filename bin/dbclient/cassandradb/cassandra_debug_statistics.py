
from cassandra.cluster import Cluster

from bin import global_settings
from bin.commons.logger import DBLoaderLogger
from bin.dbclient.cassandradb.cassandra_query_manager import CassandraQueries


class CassandraDebugStatistics:
    def __init__(self):
        self.logger = DBLoaderLogger.get_instance(CassandraDebugStatistics.__name__)
        self.configuration = global_settings.configuration_reader
        self.cluster = None
        self.session = None
        self.prepared_update_statement = None
        
    def start_capture(self):
        self.logger.debug("Starting Cassandra debug statistics...")
        if not self.__connect():
            return False
                
        if not self.__prepare_schema():
            return False
            
        if not self.__create_prepare_statements():
            return False
        
        self.logger.info("Successfully started Cassandra debug statistics")
        return True

    def __connect(self):
        nodes = [self.configuration.get_db_cluster_connection_point()]
        self.logger.debug("Connecting with cluster connection nodes [ %s ]..." % nodes)
        # Here we setup our database connection
        self.cluster = Cluster(nodes)
        self.cluster.protocol_version = int(self.configuration.get_db_cluster_protocol_version())
        self.cluster.port = 9042

        # A session manages connection pool for us
        self.session = self.cluster.connect()
        if self.session is None:
            self.logger.error("Failed to establish connection with cluster connection nodes: [ %s ]." % nodes)
            return False

        self.logger.info("Connection established successfully with cluster connection nodes [ %s ]." % nodes)

        return True

    def __prepare_schema(self):
        
        self.logger.debug("Creating schema for data loading...")
        try:
            if self.session:
                # Create Schema
                # Create tables; Delete previous ones 
                try:
                    self.session.execute(CassandraQueries.DROP_DB_LOADER_DEBUG_STATS_SCHEMA_STMT)
                except:
                    pass
                    
                self.session.execute(CassandraQueries.CREATE_DB_LOADER_DEBUG_STATS_SCHEMA_STMT)
                if self.check_if_valid_schema():
                    
                    # drop table if it already exists in the keyspace
                    try:
                        self.session.execute(CassandraQueries.DROP_DEBUG_STATS_TABLE_STMT)
                    except:
                        pass
                    self.session.execute(CassandraQueries.CREATE_DEBUG_STATS_TABLE_STMT)
                    
                    self.logger.info("Created schema successfully.")
                    return True
                
                else:
                    self.logger.error(
                        "KeySpace: [ %s ] does not exist or has not been discovered by the driver." % CassandraQueries.DB_LOADER_DEBUG_STATS_SCHEMA)
                    return False
            else:
                self.logger.error("Invalid cluster session. Failed to create schema.")
                return False
            
        except Exception as err:
            self.logger.error(err)
            return False

    def __create_prepare_statements(self):
        self.prepared_update_statement = self.session.prepare(CassandraQueries.UPDATE_DEBUG_STATS_TABLE_STMT)
        return True
    
    # FIXME : capture result
    def udpate(self, count):
        try:
            self.session.execute(self.prepared_update_statement, (count,))
        except Exception as err:
            print "Failed to update count"
            print err
    
    def stop_capture(self):
        if self.session:
            self.logger.debug("Stopping debug statistics...")
            self.session.cluster.shutdown()
            self.session.shutdown()            
            self.logger.info("Successfully stopped debug statistics.")
            
    def check_if_valid_schema(self):
        try:
            # This method return meta object; we can use this in future to get information
            # about the KeySpace
            self.cluster.metadata.keyspaces[CassandraQueries.DB_LOADER_DEBUG_STATS_SCHEMA]
        except KeyError:
            return False
        else:
            return True