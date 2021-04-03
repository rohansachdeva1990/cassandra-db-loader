from Queue import Queue
import threading

from bin.executor.batch_generator import BatchGeneratorForSubscriberWithoutDependents, \
    BatchGeneratorForSubscriberWithDependents, BatchGeneratorForDependents
from bin.commons.key_saver import CustomerKeySaver


class StagingAreaForSubscribersWithoutDependents:
    
    def __init__(self, subscriber_group, batch_loader, stop_event, configuration):
        self.batch_loader = batch_loader
        self.subscriber_group = subscriber_group
        self.customer_key_saver = CustomerKeySaver(self.subscriber_group.get_subscriber_group_type())
        self.batch_generator = BatchGeneratorForSubscriberWithoutDependents(subscriber_group, configuration, self.customer_key_saver)
        self.stop_event = stop_event
        
    def start(self):
        while(not self.batch_generator.is_finished() and not self.stop_event.is_set()):
            prepared_batch = self.batch_generator.prepare()
            if prepared_batch:               
                self.batch_loader.put_batch(prepared_batch)
            else:
                break
        self.customer_key_saver.save()

class StagingAreaForSubscribersWithDependents:    
    
    def __init__(self, subscriber_group, batch_loader, stop_event, configuration):
        self.batch_loader = batch_loader
        self.subscriber_group = subscriber_group
        self.stop_event = stop_event
        self.dependent_processing_queue = Queue()
        self.configuration = configuration
        self.customer_key_saver = CustomerKeySaver(self.subscriber_group.get_subscriber_group_type())
        self.batch_generator = BatchGeneratorForSubscriberWithDependents(subscriber_group, self.dependent_processing_queue, self.configuration, self.customer_key_saver)
        
    def start(self):
        self.__start_intenal_threads()
        while(not self.batch_generator.is_finished() and not self.stop_event.is_set()):
            prepared_batch = self.batch_generator.prepare()
            if prepared_batch:
                self.batch_loader.put_batch(prepared_batch)
            else:
                break
            
            self.customer_key_saver.save()
        # Wait on queue
        self.dependent_processing_queue.join()
        
    def __start_intenal_threads(self):
        subscriber_group_dep_list = self.subscriber_group.get_subscriber_group_dep_list()
        for subscriber_dep_group_info  in subscriber_group_dep_list:
            for _ in range(10):
                # Daemon threads
                th = DependentPreparator(self.dependent_processing_queue,
                                         self.stop_event,
                                         subscriber_dep_group_info,
                                         self.batch_loader,
                                         self.configuration)
                th.start()

class DependentPreparator(threading.Thread):

    def __init__(self, queue, stop_event, subscriber_dep_group_info, batch_loader, configuration):
        threading.Thread.__init__(self)
        self.queue = queue
        self.stop_event = stop_event
        self.setDaemon(True)
        self.batch_loader = batch_loader
        self.batch_generator = BatchGeneratorForDependents(subscriber_dep_group_info, configuration) 
        
    def run(self):
        try:
            while not self.stop_event.is_set():
                try:
                    number_tuple = self.queue.get(True, 2)
                    self.__process(number_tuple)
                    self.queue.task_done()
                except:
                    pass
        except Exception:
            self.__flush(True)
            
        self.__flush()
        
    def __process(self, number_tuple):
        customer_key = number_tuple.__getitem__(0)
        inherited_range_value = number_tuple.__getitem__(1)
        while(not self.batch_generator.is_finished() and not self.stop_event.is_set()):
            prepared_batch = self.batch_generator.prepare(inherited_range_value, customer_key)
            if prepared_batch:
                self.batch_loader.put_dependent_batch(prepared_batch)

    def __flush(self, force_stop = False):
        # Flush only when, stop event is set or some exception has occurred to force stop it 
        if (self.stop_event.is_set() or force_stop):
            while not self.queue.empty():
                self.queue.get(True, 1)
                self.queue.task_done()