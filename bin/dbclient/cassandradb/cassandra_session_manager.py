from multiprocessing import Pool
import multiprocessing
import os
import threading
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

from bin.commons.logger import DBLoaderLogger
from bin.dbclient.cassandradb.cassandra_debug_statistics import CassandraDebugStatistics
from bin.dbclient.cassandradb.cassandra_query_manager import CassandraQueries
from bin.dbclient.db_client import DbClientException
from cassandra.connection import ConnectionException

class CassandraSessionPoolManager:    
    def __init__(self, configuration):
        self.logger = DBLoaderLogger.get_instance(CassandraSessionPoolManager.__name__)
        self.configuration = configuration
        self.no_of_connection_pool = self.configuration.get_db_no_of_connection_pools()
        self.debug_stats_enabled = self.configuration.is_debug_stats_enabled()
        
    def initialize(self, query_manager):
        self.logger.debug("Initializing Cassandra Session Pool Manager with [ %d ] session pools instances..." % self.no_of_connection_pool)
        manager = multiprocessing.Manager()
        
        self.close_event = manager.Event() # For closing all the sessions
        self.batch_queue = manager.Queue() # Single queues from which batches will be processed from

        self.cassandra_client_debug_statistics = None
        if self.debug_stats_enabled:
            self.cassandra_client_debug_statistics = CassandraDebugStatistics()
                    
        self.session_pool_map = {}
        for connection_pool_index in range(self.no_of_connection_pool):
            self.session_pool_map[connection_pool_index] = CassandraSessionPool(self.batch_queue, self.configuration, query_manager, 
                                                                                self.close_event, self.cassandra_client_debug_statistics)
                                
        self.logger.info("Successfully initialized Cassandra Session Pool Manager." )
        
    def start(self):
        self.logger.debug("Starting Cassandra Session Pool Manager...")
        status = False
        
        for connection_pool_index in range(self.no_of_connection_pool):    
            self.logger.debug("Starting Session Pool - [ %d ]..." % connection_pool_index)
            cassandra_session_pool = self.session_pool_map[connection_pool_index]
            status = cassandra_session_pool.start()
            if not status:
                self.logger.warn("Failed to start Session Pool - [ %d ]..." % connection_pool_index)
                continue
            else:
                self.logger.info("Started Session Pool - [ %d ]" % connection_pool_index)
            
        if status:
            self.logger.info("Successfully started Cassandra Session Pool Manager.")
            if self.cassandra_client_debug_statistics:
                if not self.cassandra_client_debug_statistics.start_capture():
                    self.logger.error("Failed to start debug statistics.. Closing all")
                    status = False
        else :
            self.logger.error("Failed to start Cassandra Session Pool Manager...")
            
        return status
            
    def stop(self):
        self.logger.info("Waiting for batches to be loaded...")
        self.batch_queue.join() # Wait for all batches to arrive
        
        self.logger.debug("All batches processed successfully. Now stopping Cassandra Session Pool Manager...")
        self.close_event.set()
        
        for connection_pool_index in range(self.no_of_connection_pool):
            self.logger.debug("Stopping Session Pool - [ %d ]..." % connection_pool_index)
            cassandra_session_pool = self.session_pool_map[connection_pool_index]
            cassandra_session_pool.stop()
            self.logger.info("Successfully stopped Session Pool - [ %d ]" % connection_pool_index)

        if self.cassandra_client_debug_statistics:
            self.cassandra_client_debug_statistics.stop_capture()      
        
        self.logger.debug("Successfully stopped Cassandra Session Pool Manager.")
            
    def get_pool_processing_handle(self):
        return self.batch_queue

class CassandraSessionPool:
    
    def __init__(self, batch_queue, configuration, query_manager, close_event, cassandra_client_debug_statistics):
        self.logger = DBLoaderLogger.get_instance(CassandraSessionPool.__name__)
        
        self.batch_queue = batch_queue
        self.configuration = configuration
        self.query_manager = query_manager
        self.close_event = close_event
        self.cassandra_client_debug_statistics = cassandra_client_debug_statistics
        
        self.number_of_sessions = configuration.get_db_no_of_sessions()
        self.connection_guard_semaphore = multiprocessing.BoundedSemaphore(self.number_of_sessions)
        self.session_pool = None

    def start(self):
        status = False
        try:
            self.session_pool = Pool(processes=self.number_of_sessions, initializer=self._setup, initargs=(self.query_manager, self.connection_guard_semaphore,
                                                                                                           self.close_event, self.configuration, ))
            self.__wait_for_sessions_to_be_active()
            internal_batch_loading_thread = self.InternalBatchLoadingThread(self.batch_queue, 
                                                                            self.InternalBatchLoader(self.session_pool,
                                                                                                     self.cassandra_client_debug_statistics))        
            internal_batch_loading_thread.start()
            status = True
        except Exception as err:
            self.logger.error(err)
        return status

    @classmethod
    def _setup(cls, query_manager, connection_guard_semaphore, close_event, configuration):
        # FIXME: Add a way to kill the process, when we fail to establish connection        
        cls.casssandra_session = CassandraSession( query_manager, connection_guard_semaphore, close_event, configuration)
        cls.casssandra_session.connect()

    @classmethod
    def execute(cls, subscriber_group_index, subscriber_group_type, subscriber_group_name, params):
        return cls.casssandra_session.execute(subscriber_group_index, subscriber_group_type, subscriber_group_name, params)
    
    def stop(self):
        self.__wait_for_sessions_to_be_closed()
        if self.session_pool:        
            self.session_pool.close()
            self.session_pool.join()
            
    def __wait_for_sessions_to_be_active(self):
        # Waiting for connections to come up
        retry_count = 1
        are_all_sessions_up = False
        while retry_count <= 5:
            if self.connection_guard_semaphore.get_value() == 0:
                self.logger.info("Connections established successfully having [ %d ] active sessions." % self.number_of_sessions)
                are_all_sessions_up = True
                break
            else :
                self.logger.debug("Waiting for all the sessions to come up. Current active sessions: [ %d ]" % (self.number_of_sessions - self.connection_guard_semaphore.get_value()))
                time.sleep(1 * retry_count)
                retry_count = retry_count + 1
         
        if not are_all_sessions_up:
            raise DbClientException("Only [ %d ] active connections were established out of [ %d ] requested." % ((self.number_of_sessions - self.connection_guard_semaphore.get_value()), self.number_of_sessions))
        
    def __wait_for_sessions_to_be_closed(self):
        retry_count = 1
        are_all_session_closed = False
        while retry_count <= 5:
            if (self.connection_guard_semaphore.get_value() == self.number_of_sessions):
                self.logger.info ("Connections closed successfully.")
                are_all_session_closed = True
                break
            else:
                self.logger.debug("Waiting for all the sessions to close. Current active sessions remaining: [ %d ]" % (self.number_of_sessions - self.connection_guard_semaphore.get_value()))
                time.sleep(1 * retry_count)
                retry_count = retry_count + 1

        if not are_all_session_closed:
            self.logger.warn("Some connections still open. No. of connections still open: [ % d ]" % (self.number_of_sessions - self.connection_guard_semaphore.get_value()))
    
    
    class InternalBatchLoader:
        def __init__(self, session_pool, cassandra_debug_statistics):
            self.logger = DBLoaderLogger.get_instance(CassandraSessionPool.InternalBatchLoader.__name__)
            self.session_pool = session_pool
            self.cassandra_debug_statistics = cassandra_debug_statistics
            
        def load(self, batch):
            try:
                params = batch.get_params() 
                self.logger.debug("Loading batch of size: [ %d ] ..." % params .__len__())
                start_time = time.time()
                
                # Parse the batch
                job_args = [(batch.get_subscriber_group_index(), 
                             batch.get_subscriber_group_type(),
                             batch.get_subscriber_group_name(),
                             params[n:n+100]) for n in range(0, len(params), 100)]
                
                results = self.session_pool.map(cassandra_session_pool_worker, job_args)
                if self.cassandra_debug_statistics and results:
                    self.cassandra_debug_statistics.udpate(sum(results))
                    
                end_time = time.time()
                self.logger.debug("Time taken to load batch is [ %d ] seconds." % (end_time - start_time))
                
            except Exception as err:
                self.logger.error("Exception while loading batch... Error: [ %s ]" % str(err))
    
    class InternalBatchLoadingThread(threading.Thread):
        def __init__(self, batch_queue, batch_loader):
            self.logger = DBLoaderLogger.get_instance(CassandraSessionPool.InternalBatchLoadingThread.__name__)
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.batch_queue = batch_queue
            self.batch_loader = batch_loader
            
        def run(self):
            self.logger.debug("Internal batch loader thread started...")
            try:
                while True:
                    try:
                        batch = self.batch_queue.get(True, 3)
                        self.batch_loader.load(batch)
                        self.batch_queue.task_done()
                    except:
                        pass # Timeout; Will try again
            except Exception as err:
                self.logger.error(err)
                self.__flush() 
            
        def __flush(self):
            while not self.batch_queue.empty():
                self.batch_queue.get(True, 1)
                self.batch_queue.task_done()

def cassandra_session_pool_worker(arguments):
    s_g_i, s_g_t, s_g_n, params= arguments
    return CassandraSessionPool.execute(s_g_i, s_g_t, s_g_n, params)

class CassandraSession:
    def __init__(self, query_manager, connection_guard_semaphore, close_event, configuration):
        self.query_manager = query_manager
        self.connection_guard_semaphore = connection_guard_semaphore
        self.close_event = close_event
        self.configuration = configuration
        
        self.__initialize_configuration()
    
    def __initialize_configuration(self):    
        self.debug_stats_enabled = self.configuration.is_debug_stats_enabled()        
        self.nodes = [self.configuration.get_db_cluster_connection_point()]
        
        self.prepared_query_store = None
        self.cluster = None
        self.session = None
        self.session_using_lock = threading.RLock() # To ensure safe disconnection of sessions
        self.session_clean_up_thread = self.SessionCleanUpThread(self, self.close_event)
        
    def connect(self):
        status = False
        retry_count = 1
        self.connection_guard_semaphore.acquire()
        
        while(retry_count <= 3):
        
            try:
                # Setting up database connection
                self.cluster = Cluster(self.nodes)
                self.cluster.protocol_version = int(self.configuration.get_db_cluster_protocol_version())
                self.cluster.port = 9042
                
                # A session manages connection pool for us
                self.session = self.cluster.connect(keyspace=CassandraQueries.PSUSER_DATA_SCHEMA)
                if self.session is None:
                    raise DbClientException("For process with id: [ %s ], Failed to establish connection with cluster connection nodes: [ %s ]." % (os.getpid(), self.nodes))

                self.prepared_query_store = self.query_manager.get_instance(self.session)

                # Only start clean up thread, when we have valid session...
                self.session_clean_up_thread.start()                        
                print("Connection established successfully with cluster connection nodes [ %s ] for process id: [ %s ]." % (self.nodes, os.getpid()))
                status = True
                break
            
            except Exception as err:
                print err
                print "Now, retrying to create cluster session for process id: [ %s ]..." % os.getpid()   
                time.sleep(1 * retry_count)
                retry_count = retry_count + 1
                if(retry_count > 3):
                    self.connection_guard_semaphore.release()
                
        return status    
                    
    def close(self):
        self.session_using_lock.acquire()
        try:
            if self.session:
                self.session.cluster.shutdown()
                self.session.shutdown()
#                 print "session closed for process id %s" % os.getpid()
        finally:
            self.connection_guard_semaphore.release()
            self.session_using_lock.release()
        
    def execute(self, subscriber_group_index, subscriber_group_type, subscriber_group_name, params):
        success_count = 0
        try:
            self.session_using_lock.acquire()
               
            prepared_statement = self.prepared_query_store.get_prepared_statement( subscriber_group_index, subscriber_group_type, subscriber_group_name)
            if prepared_statement:
                results = execute_concurrent_with_args(self.session, prepared_statement, params)
                if self.debug_stats_enabled:
                    success_count = self.__get_execution_success_count(results)
                       
        except Exception as err:
            print err
        finally:
            self.session_using_lock.release() 
            
        return success_count   
        
    def __get_execution_success_count(self, results):
        successful_executions = 0
        if results :
            successful_executions = results.__len__()
            for result in results:
                is_success = result.__getitem__(0) 
                if not is_success:
                    successful_executions = successful_executions - 1 
        return successful_executions
    
    # Internal thread for cleaning up cluster session
    class SessionCleanUpThread(threading.Thread):        
        def __init__(self, cassandra_session, cassandra_session_close_event):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.cassandra_session_close_event = cassandra_session_close_event
            self.cassandra_session = cassandra_session

        # Wait indefinitely to close a session
        def run(self):
            self.cassandra_session_close_event.wait()
            self.cassandra_session.close()
