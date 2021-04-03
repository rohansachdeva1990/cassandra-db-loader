import multiprocessing
import time

import concurrent.futures

from bin import global_settings
from bin.commons.logger import DBLoaderLogger
from bin.executor.batch_loader import BatchLoader, BatchLoaderWithDependents
from bin.executor.staging_area import StagingAreaForSubscribersWithDependents, \
    StagingAreaForSubscribersWithoutDependents
from bin.commons.key_saver import CustomerKeySaver


class ClientExecutionManager:

    def __init__(self):
        self.logger = DBLoaderLogger.get_instance(ClientExecutionManager.__name__)
        manager = multiprocessing.Manager()
        self.stop_event = manager.Event()
        self.configuration = global_settings.configuration_reader

    def execute(self, subscribers_data, batch_queue):
        self.logger.debug("Starting to execute with [ %d ] number of subscriber groups... " % subscribers_data.get_count())
        start_time_stamp = time.time()
        with concurrent.futures.ProcessPoolExecutor(max_workers = subscribers_data.get_count()) as executor:
            for subscriber_group in subscribers_data.get_subscriber_group_list():
                executor.submit(client_executor_routine, ( ClientExecutor(subscriber_group, self.stop_event, batch_queue, self.configuration)))

        self.logger.info("Finished processing all the subscriber groups in [ %d ] seconds."% (time.time() - start_time_stamp))
                    
    def shutdown(self):
        if not self.stop_event.is_set():
            self.stop_event.set()

# Catch keyboard interupt
def client_executor_routine(client_executor):
    try:
        client_executor.load()
    except KeyboardInterrupt:
        pass

class ClientExecutor:

    def __init__(self, subscriber_group, stop_event, batch_queue, configuration):
        self.subscriber_group = subscriber_group
        self.stop_event = stop_event
        self.batch_queue = batch_queue
        self.configuration = configuration
        
    def load(self):
        client_executor_helper = None
        try:            
            if not self.subscriber_group.is_for_deletion():
                client_executor_helper = ClientExecutorHelperForInsertion(self.subscriber_group, self.stop_event, self.batch_queue, self.configuration)
            else:
                client_executor_helper = ClientExecutorHelperForDeletion(self.subscriber_group, self.stop_event, self.batch_queue, self.configuration)
                
            client_executor_helper.start()
        except Exception as err:
            print(err)
        finally:
            if client_executor_helper:
                client_executor_helper.wait()    
 
class ClientExecutorHelperForInsertion:
    
    def __init__(self, subscriber_group, stop_event, batch_queue, configuration):
        self.subscriber_group = subscriber_group
        self.stop_event = stop_event
        self.staging_area = None
        self.batch_loader = None
        self.batch_queue = batch_queue
        self.configuration = configuration
        
        self.__init()
        
    def __init(self):        
        if self.subscriber_group.has_dependents():
            self.batch_loader = BatchLoaderWithDependents(self.subscriber_group, self.stop_event, self.batch_queue, self.configuration)
            self.staging_area = StagingAreaForSubscribersWithDependents(self.subscriber_group, self.batch_loader, self.stop_event, self.configuration)
        else:
            self.batch_loader = BatchLoader(self.subscriber_group, self.stop_event, self.batch_queue, self.configuration)
            self.staging_area = StagingAreaForSubscribersWithoutDependents(self.subscriber_group, self.batch_loader, self.stop_event, self.configuration)
            
        self.batch_loader.start()
            
    def start(self): 
        self.staging_area.start()
        
    def wait(self):
        self.batch_loader.wait()
        
        
class ClientExecutorHelperForDeletion:
    def __init__(self,subscriber_group, stop_event, batch_queue, configuration):
        self.subscriber_group = subscriber_group
        self.stop_event = stop_event
        self.batch_queue = batch_queue
        self.configuration = configuration
        self.batch_loader = None
        
    def start(self):
        customer_key_saver = CustomerKeySaver(self.subscriber_group.get_subscriber_group_type())
        customer_key_saver.read()
        customer_key_list = customer_key_saver.get_customer_key_list()
        
        customer_key_list_size = customer_key_list.__len__()
        if customer_key_list_size > 0:
            self.subscriber_group.set_subscriber_group_limit(customer_key_list_size)
            self.batch_loader = BatchLoader(self.subscriber_group, self.stop_event, self.batch_queue, self.configuration)
            self.batch_loader.start()
            self.batch_loader.put_batch((self.subscriber_group.get_subscriber_group_index(), 
                                     self.subscriber_group.get_subscriber_group_name(), 
                                     self.subscriber_group.get_subscriber_group_type(),
                                     customer_key_list))
    def wait(self):
        if self.batch_loader:
            self.batch_loader.wait()
