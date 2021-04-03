from Queue import Queue
import threading

# Replace thread by processes and queue by multiprocessing queue
class BatchLoader:

    def __init__(self, subscriber_group, stop_event, batch_queue, configuration):
        self.batch_loading_queue = Queue()
        self.batch_loading_thread = BatchLoadingThread(self.batch_loading_queue,
                                                       subscriber_group.get_subscriber_group_limit(),
                                                       stop_event,
                                                       batch_queue, 
                                                       configuration)         
    def put_batch(self, data):
        self.batch_loading_queue.put(data)

    def wait(self):
        self.batch_loading_queue.join()
            
    def start(self):
        self.batch_loading_thread.start()


class BatchLoaderWithDependents(BatchLoader):

    def __init__(self, subscriber_group, stop_event, batch_queue, configuration):
        BatchLoader.__init__(self, subscriber_group, stop_event, batch_queue, configuration)
        
        self.batch_loading_queue_for_dep = Queue()
        total_num_tuples = subscriber_group.get_subscriber_group_dep_limit() * subscriber_group.get_subscriber_group_limit()
        self.batch_loading_thread_for_dep = BatchLoadingThread(self.batch_loading_queue_for_dep,
                                                               total_num_tuples,
                                                               stop_event,
                                                               batch_queue, configuration)
    def put_dependent_batch(self, data):
        self.batch_loading_queue_for_dep.put(data)

    def wait(self):
        self.batch_loading_queue.join()
        self.batch_loading_queue_for_dep.join()
    
    def start(self):
        self.batch_loading_thread.start()
        self.batch_loading_thread_for_dep.start()

class Batch:

    def __init__(self, params):
        self.params = params
        self.__init()
        
    def __init(self):
        self.subscriber_group_index = self.params.__getitem__(0)
        self.subscriber_group_name = self.params.__getitem__(1)
        self.subscriber_group_type = self.params.__getitem__(2)
        self.prepared_batch = self.params.__getitem__(3)

    def get_subscriber_group_index(self):
        return self.subscriber_group_index;
         
    def get_subscriber_group_type(self):
        return self.subscriber_group_type
        
    def get_subscriber_group_name(self):
        return self.subscriber_group_name
    
    def get_params(self):
        return self.prepared_batch


#FIXME : Major refactoring required
class BatchLoadingThread(threading.Thread):
 
    def __init__(self, 
                 batch_loading_queue, 
                 total_num_of_insertion_required, 
                 stop_event,
                 batch_queue, configuration):
        threading.Thread.__init__(self)

        self.setDaemon(True)  # When event is not set
        self.batch_loading_queue = batch_loading_queue
        self.total_num_of_insertion_required = total_num_of_insertion_required
        self.statements_and_params_save_list = []
        self.db_batch_size = int(configuration.get_db_batch_size())
        self.stop_event = stop_event
    
        if self.total_num_of_insertion_required <= self.db_batch_size:
            self.db_batch_size = self.total_num_of_insertion_required
        
        self.client_processing_queue = batch_queue

    def run(self):
        try:
            while not self.stop_event.is_set():

                try:
                    data_list = self.batch_loading_queue.get(True, 2)

                    # FIXME: This is a work around. convert to batch - 
                    # process tuple; Performance impacted
                    batch = Batch(data_list)
                    self.process_and_load(batch.get_subscriber_group_index(),
                                          batch.get_subscriber_group_name(),
                                          batch.get_subscriber_group_type(), 
                                          batch.get_params())

                    self.batch_loading_queue.task_done()
                except:
                    pass

        except Exception:
            self.__flush(True)
        self.__flush()
        
    def __flush(self, force_stop = False):
        if (self.stop_event.is_set() or force_stop):
            while not self.batch_loading_queue.empty():
                self.batch_loading_queue.get(True, 1)
                self.batch_loading_queue.task_done()

    # FIXME: Refractor - Divide the code
    def process_and_load(self, s_g_i, s_g_n, s_g_t, data_list):
        
        data_list_length = data_list.__len__()
        current_length = self.statements_and_params_save_list.__len__()

        # Check if data list size is same as maximum batch size ?
        if data_list_length == self.db_batch_size:
#             print "data list equals than batch size"
            self.__load((s_g_i, s_g_n, s_g_t, data_list))

        # Check if data list size is less than maximum batch size ?
        elif data_list_length < self.db_batch_size:
#             print "data list less than batch size"

            to_be_ready_batch_len = data_list_length + current_length

            # Check if batch is good enough to be loaded ?
            # Case - No, Just save
            if to_be_ready_batch_len < self.db_batch_size:
                self.statements_and_params_save_list.extend(data_list)

            # Case - Load and save rest
            elif to_be_ready_batch_len > self.db_batch_size:
                diff = self.db_batch_size - current_length
                self.statements_and_params_save_list.extend(data_list[:diff])
                self.__load((s_g_i, s_g_n, s_g_t,self.statements_and_params_save_list))
                self.statements_and_params_save_list = data_list[diff:]

            # Case - Load
            else:
                #equal size
                self.statements_and_params_save_list.extend(data_list)
                self.__load((s_g_i, s_g_n, s_g_t,self.statements_and_params_save_list))
                self.statements_and_params_save_list[:] = []
        
        elif data_list_length > self.db_batch_size:
            
#             print "data list greater than batch size"

            # Here data list size is greater than batch size. So, we will first load
            # and then copy the remaining.
            self.__load((s_g_i, s_g_n, s_g_t,data_list[:self.db_batch_size]))
            list2 = data_list[self.db_batch_size:]
            self.total_num_of_insertion_required -= self.db_batch_size
            self.process_and_load(s_g_i, s_g_n, s_g_t, list2)
            
        else:
            pass
#             print "data_list is equal to 0"
#             
#             if (list2.__len__() + current_length) < self.db_batch_size:
#                 self.statements_and_params_save_list.extend(data_list)
#             else:# Incorrect Logic
# 
#                 diff = self.db_batch_size - current_length
#                 self.statements_and_params_save_list.extend(list2[:diff])
#                 self.__load((s_g_i, s_g_n, s_g_t,self.statements_and_params_save_list))
#                 self.statements_and_params_save_list = list2[diff:]

        self.total_num_of_insertion_required -= data_list_length
        if (self.total_num_of_insertion_required == 0 and self.statements_and_params_save_list.__len__() > 0):
            self.__load( (s_g_i, s_g_n, s_g_t, self.statements_and_params_save_list))
            self.statements_and_params_save_list[:] = []
       
     
    def __load(self, value):
        self.client_processing_queue.put(Batch(value))
