from abc import abstractmethod
import abc
import datetime
import uuid

from bin.commons.utils import CommonUtils
from bin.commons.xml_parser import XMLConstants

class BatchGenerator:
    __metaclass__ = abc.ABCMeta
    
    def __init__(self, subscriber_group, configuration):
        self.db_batch_size = int(configuration.get_db_batch_size())
        self.subscriber_group = subscriber_group
        self.__initialize()
        
    def __initialize(self):
        self.subscriber_format = self.subscriber_group.get_subscriber_group_format()
        self.subscriber_limit = self.subscriber_group.get_subscriber_group_limit()
        self.subscriber_group_index = self.subscriber_group.get_subscriber_group_index()
        self.subscriber_group_type = self.subscriber_group.get_subscriber_group_type()
        self.subscriber_group_name = self.subscriber_group.get_subscriber_group_name()
        
        place_holder_list = self.subscriber_group.get_subscriber_group_placeholder_data_list()
        self.place_holder_data = CommonUtils.get_first_from_iterable(place_holder_list)
        self.max_range_value = self.subscriber_limit + self.place_holder_data.get_start_range()
        self.place_holder_range_save_map = {}
        self.remaining_records_to_prepare = self.subscriber_limit
 
    def is_finished(self):
        
        # Error
        if self.remaining_records_to_prepare > 0:
            return False
        else:
            # When finished, reset it to original limit for processing next request
            self.remaining_records_to_prepare = self.subscriber_limit
            return True
            

    def get_start_range(self):
        # Check whether there is a range value we want to continue from ... 
        start_range = self.place_holder_data.get_start_range()
        place_holder_tag = self.place_holder_data.get_place_holer_tag()
        if self.place_holder_range_save_map.__contains__(place_holder_tag):
            start_range = self.place_holder_range_save_map.__getitem__(place_holder_tag)
            self.place_holder_range_save_map.__delitem__(place_holder_tag)
                    
        return start_range
    
    def get_end_range(self, current_range):
        diff = self.max_range_value - current_range
        if diff > self.db_batch_size:
            end_range = current_range + self.db_batch_size
            self.place_holder_range_save_map[self.place_holder_data.get_place_holer_tag()] = end_range
        else:
            end_range = current_range + diff    
        return end_range
    
    def get_number_for_replacing(self, range_value):
        return CommonUtils.get_padded_number(range_value, self.place_holder_data.get_place_holder_tag_length())
    
    def get_tuple_to_be_inserted(self, user_number_range_value, customer_key):
        updated_subscribers_format = self.subscriber_format.replace(self.place_holder_data.get_place_holer_tag(),user_number_range_value)
        return (long(updated_subscribers_format), customer_key, None)

    def add_info_to_batch(self, prepared_batch):
        if prepared_batch.__len__() > 0:
            return (self.subscriber_group_index, self.subscriber_group_name, self.subscriber_group_type, prepared_batch)
        else:
            return None
        
    @abstractmethod
    def prepare(self):        
        return []   
    
    
class BatchGeneratorForSubscriberWithoutDependents(BatchGenerator):

    def __init__(self, subscriber_group, configuration, customer_key_saver):
        BatchGenerator.__init__(self, subscriber_group, configuration)
        self.customer_key_saver = customer_key_saver
        
    def prepare(self):
        value_list = []
        start_range_value = self.get_start_range()
        end_range_value = self.get_end_range(start_range_value)
        while start_range_value < end_range_value:
            customer_key = uuid.uuid4()
            self.customer_key_saver.store(customer_key)
            user_number_range_value = self.get_number_for_replacing(start_range_value)
            prepared_tuple = self.get_tuple_to_be_inserted(user_number_range_value, customer_key)
            value_list.append(prepared_tuple)
            start_range_value +=1
            
        self.remaining_records_to_prepare -= value_list.__len__()
        return self.add_info_to_batch(value_list)
    
    
class BatchGeneratorForSubscriberWithDependents(BatchGenerator):
    
    def __init__(self, subscriber_group, queue, configuration, customer_key_saver):
        BatchGenerator.__init__(self, subscriber_group, configuration)
        self.queue = queue
        self.customer_key_saver = customer_key_saver
        
    def prepare(self):
        value_list = []
        start_range_value = self.get_start_range()
        end_range_value = self.get_end_range(start_range_value)
        while start_range_value < end_range_value:
            customer_key = uuid.uuid4()
            self.customer_key_saver.store(customer_key)
            user_number_range_value = self.get_number_for_replacing(start_range_value)
            prepared_tuple = self.get_tuple_to_be_inserted(user_number_range_value, customer_key)
    
            # Post the tuple on the queue
            self.queue.put((customer_key, user_number_range_value))
            
            value_list.append(prepared_tuple)
            start_range_value +=1
            
        self.remaining_records_to_prepare -= value_list.__len__()
        return self.add_info_to_batch(value_list)
    
        
class BatchGeneratorForDependents(BatchGenerator):
    
    def __init__(self, subscriber_group, configuration):
        BatchGenerator.__init__(self, subscriber_group, configuration)

    def prepare(self, inherited_range_value, customer_key):
    
        value_list = []
        start_range_value = self.get_start_range()
        end_range_value = self.get_end_range(start_range_value)
        while start_range_value < end_range_value:
            user_number_range_value = self.get_number_for_replacing(start_range_value)
            prepared_tuple = self.__get_tuple_to_be_inserted_for_dependent(user_number_range_value, inherited_range_value, customer_key)
            value_list.append(prepared_tuple)
            start_range_value +=1
            
        self.remaining_records_to_prepare -= value_list.__len__()
        return self.add_info_to_batch(value_list)
        
    def __get_tuple_to_be_inserted_for_dependent( self, user_number_range_value, inherited_range_value, customer_key):
        
        # Replace the inherit tag
        updated_subscribers_format = self.subscriber_format.replace(XMLConstants.INHERIT_TAG ,inherited_range_value)
        
        # Replace the number
        updated_subscribers_format = updated_subscribers_format.replace( self.place_holder_data.get_place_holer_tag(), user_number_range_value)
        return (customer_key, long(updated_subscribers_format), CommonUtils.unix_time_millis(datetime.datetime.now()))