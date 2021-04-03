import time

from bin.commons.logger import DBLoaderLogger
from bin.commons.subscriber_data_mapper import SubscriberDataFactory
from bin.commons.xml_parser import XMLParser, XMLParserException
from bin.dbclient.db_client import DbClientException
from bin.executor.client_executor import ClientExecutionManager
from bin.dbclient.cassandradb.cassandra_query_manager import CassandraQueryManager

class DatabaseLoader:

    def __init__(self):
        self.logger = DBLoaderLogger.get_instance(DatabaseLoader.__name__)
        self.client_execution_manager = ClientExecutionManager()
          
    def start_population(self, subscriber_data_path, db_client):

        self.logger.info("##---------------------- Database Loader Started ----------------------##")
        start_time_stamp = time.time()
         
        status = self.__start_population(subscriber_data_path, db_client)
            
        self.logger.info("Total time taken: [ %d ] seconds." % (time.time() - start_time_stamp))
        self.logger.info("##---------------------- Database Loader Finished ----------------------##\n")
        return status
        
    def __start_population(self, subscriber_data_path, db_client):            
        status = False
        try:
            # Reading raw subscriber data
            xmlParser = XMLParser()
            xmlParser.parse(subscriber_data_path)
            
            # Connecting to client processor; Shared by all the processes
            query_manager = CassandraQueryManager(xmlParser.get_subscribers_data_map())
            if db_client.connect(query_manager):
                # Finally executing our scenarios
                subscriber_data = SubscriberDataFactory.get_subscriber_data(xmlParser.get_subscribers_data_map())
                self.client_execution_manager.execute(subscriber_data, db_client.get_client_handle())
                status = True
            
        except (XMLParserException, DbClientException, Exception) as err:
            self.logger.error(err)
            
        finally:
            db_client.close()
                
        return status

    def start_deletion(self, db_client, subscriber_type):

        self.logger.info("##---------------------- Database Loader Started ----------------------##")
        start_time_stamp = time.time()
         
        status = self.__start_deletion(db_client, subscriber_type)
            
        self.logger.info("Total time taken: [ %d ] seconds." % (time.time() - start_time_stamp))
        self.logger.info("##---------------------- Database Loader Finished ----------------------##\n")
        return status

    def __start_deletion(self, db_client, subscriber_type):
        status = False
        try:                
            # Connecting to client processor; Shared by all the processes
            subscriber_map_for_deletion = XMLParser.get_dummy_subscriber_data_map(subscriber_type)
            query_manager = CassandraQueryManager(subscriber_map_for_deletion)
            if db_client.connect(query_manager):
                # Finally executing our scenarios
                subscriber_data = SubscriberDataFactory.get_subscriber_data_for_deletion(subscriber_map_for_deletion)
                self.client_execution_manager.execute(subscriber_data, db_client.get_client_handle())
                status = True
            
        except (XMLParserException, DbClientException, Exception) as err:
            self.logger.error(err)   
        finally:
            db_client.close()                
        return status

    def stop(self):
        if self.client_execution_manager:
            self.client_execution_manager.shutdown()