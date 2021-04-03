from bin.commons.xml_parser import XMLConstants

class SubscriberDataFactory:    

    @staticmethod
    def get_subscriber_data(subscribers_data_map):
        return SubscriberData(subscribers_data_map)

    @staticmethod
    def get_subscriber_data_for_deletion(subscribers_data_map):
        return SubscriberData(subscribers_data_map, True)

class SubscriberData:
    def __init__(self, subscribers_data_map, for_deletion = False):
        self.subscribers_data_map = subscribers_data_map
        self.subscriber_info_list = []
        
        for subscribers_group_index in self.subscribers_data_map:
            self.subscriber_info_list.append(SubscriberGroup(subscribers_group_index, self.subscribers_data_map[subscribers_group_index], for_deletion))
        
    def get_subscriber_group_list(self):
        return self.subscriber_info_list

    def get_count(self):
        return self.subscribers_data_map.__len__()

class SubscriberGroup:
    def __init__(self, subscriber_group_index, subscriber_info_map, for_deletion):
        self.subscriber_group_index = subscriber_group_index
        self.subscriber_info = SubscriberInfo(subscriber_info_map)
        self.for_deletion = for_deletion
        self.subscriber_group_dep_list = self.__create_subscriber_group_dep_list()

    def get_subscriber_group_index(self):
        return self.subscriber_group_index

    def get_subscriber_group_name(self):
        return self.subscriber_info.get_node_name()

    def get_subscriber_group_type(self):
        return self.subscriber_info.get_type()

    def get_subscriber_group_limit(self):
        return self.subscriber_info.get_limit()

    def set_subscriber_group_limit(self, limit_value):
        self.subscriber_info.set_limit(limit_value)

    def get_subscriber_group_dep_limit(self):
        subscribers_dep_tuples_count = 0
        # TODO: Now, handling single dependent
        for subscriber_group_data in self.subscriber_group_dep_list:
            subscribers_dep_tuples_count+= subscriber_group_data.get_subscriber_group_limit();
            break
            
        return subscribers_dep_tuples_count

    def get_subscriber_group_format(self):
        return self.subscriber_info.get_format()

    def get_subscriber_group_placeholder_data_list(self):
        return self.subscriber_info.get_placeholder_data_list()

    def get_subscriber_group_dep_list(self):
        return self.subscriber_group_dep_list

    def has_dependents(self):
        return self.subscriber_info.has_dependents()

    def get_total_number_of_tuples(self):
        subscribers_dep_tuples_count = 1
        for subscriber_group_data in self.subscriber_group_dep_list:
            subscribers_dep_tuples_count+= subscriber_group_data.get_subscriber_group_limit();
        return (self.get_subscriber_group_limit() * subscribers_dep_tuples_count)

    def __create_subscriber_group_dep_list(self):
        subscriber_dependent_group_list = []
        dep_list = self.subscriber_info.get_dep_list()
        if dep_list is not None:
            for subscriber_dependent_info_map in dep_list:
                # Recursive Call
                subscriber_dependent_group_list.append(SubscriberGroup(self.subscriber_group_index, subscriber_dependent_info_map, False))                
        return subscriber_dependent_group_list

    def is_for_deletion(self):
        return self.for_deletion


class SubscriberInfo:
    def __init__(self, subscriber_info_map):
        self.subscriber_info_map = subscriber_info_map
        self.__init()

    def __init(self):
        self.node_name = self.subscriber_info_map.__getitem__(XMLConstants.XML_NODE_NAME_TAG)
        self.subscriber_type = self.subscriber_info_map.__getitem__(XMLConstants.TYPE_TAG)
        
        self.subscriber_limit= None
        if self.subscriber_info_map.__getitem__(XMLConstants.LIMIT_TAG):
            self.subscriber_limit = int(self.subscriber_info_map.__getitem__(XMLConstants.LIMIT_TAG))
        
        self.subscriber_format = self.subscriber_info_map.__getitem__(XMLConstants.FORMAT_TAG)
        self.subscriber_placeholder_map = self.subscriber_info_map.__getitem__(XMLConstants.PLACE_HOLDER_TAG)
        self.subscriber_dep_list =  self.subscriber_info_map.__getitem__(XMLConstants.SUBSCRIBER_DEPENDENTS)

        self.placeholder_data_list = self.__create_place_holder_list()
    
    def get_node_name(self):
        return self.node_name

    def get_type(self):
        return self.subscriber_type
    
    def get_limit(self):
        return self.subscriber_limit

    def set_limit(self, limit_value):
        self.subscriber_limit = limit_value

    def get_format(self):
        return self.subscriber_format

    def get_placeholder_map(self):
        return self.subscriber_placeholder_map

    def get_dep_list(self):
        return self.subscriber_dep_list

    def get_placeholder_data_list(self):
        return self.placeholder_data_list

    def has_dependents(self):
        if self.subscriber_dep_list.__len__() < 1 :
            return False
        else:
            return True

    def __create_place_holder_list(self):
        placeholder_data_list = []
        if self.get_placeholder_map():
            for place_holder_tuple, place_holder_range_tuple in self.get_placeholder_map().iteritems():
                placeholder_data_list.append(PlaceHolderData(place_holder_tuple, place_holder_range_tuple))
                
        return placeholder_data_list

class PlaceHolderData:

    def __init__(self, place_holder_tuple, place_holder_range_tuple):
        self.place_holder_tuple = place_holder_tuple
        self.place_holder_range_tuple = place_holder_range_tuple
        self.__init()

    def __init(self):
        self.place_holder_tag = self.place_holder_tuple.__getitem__(0)
        self.place_holder_tag_length = int(self.place_holder_tuple.__getitem__(1))
        self.start_range = int (self.place_holder_range_tuple.__getitem__(0))
        self.end_range = int (self.place_holder_range_tuple.__getitem__(1)) + 1

    def get_start_range(self):
        return self.start_range

    def get_end_range(self):
        return self.end_range

    def get_place_holer_tag(self):
        return self.place_holder_tag

    def get_place_holder_tag_length(self):
        return self.place_holder_tag_length
