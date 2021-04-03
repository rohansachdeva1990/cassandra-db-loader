from collections import OrderedDict
import re

from bin.commons.logger import DBLoaderLogger
import xml.etree.ElementTree as ET


class XMLConstants:

    # -------------------- Static Data Members

    ROOT_ELEMENT_TAG = "subscriberData"
    CHILD_ELEMENT_TAG = "subscribers"
    SUBSCRIBERS_TAG = "subscribers"
    XML_NODE_NAME_TAG = "nodeName"
    DELETE_NODE_NAME_TAG = "deleteNode"
    TYPE_TAG = "type"
    LIMIT_TAG = "limit"
    FORMAT_TAG = "format"
    VALUE_TAG = "value"
    DEFAULT_TAG = "default"
    PLACE_HOLDER_TAG = "placeholder"
    PLACE_HOLDER_SEARCH_REGEX = "%[^(?!%)]+%"
    INHERIT_TAG = "_INHERIT_"
    SEMI_COLON_TAG = ";"
    COLON_TAG = ":"
    EQUALS_TAG = "="
    SUBSCRIBER_DEPENDENTS = "subscriberDependents"
    CALLING_SUBSCRIBER_TAG = "callingSubscribers"
    SCENARIO_TYPE_ALLOWED = "0"
    SCENARIO_TYPE_BLOCKED = "1"
    SCENARIO_TYPE_SCREENING_ALLOWED = "2"
    SCENARIO_TYPE_SCREENING_BLOCKED = "3"


class XMLParser:

    def __init__(self):
        self.logger = DBLoaderLogger.get_instance(XMLParser.__name__)
        self.subscribers_details_map = {}

    def parse(self, filename ):

        self.logger.debug("Parsing started for XML file: [ %s ]." % filename)
        
        # Parses an XML document into an element tree
        tree = ET.parse(filename)

        # Get subscriber_data element tag data
        subscriber_data = tree.getroot()
        if subscriber_data is None:
            raise XMLParserException("Parsing failed... [ " + XMLConstants.ROOT_ELEMENT_TAG + " ] tag not found.")

        subscribers_info_counter = 0
        for subscribers in subscriber_data:

            self.logger.debug("Generating subscriber info map for entry [ %d ] ..." % subscribers_info_counter)
            
            subscribers_info_map = self.__generate_subscriber_info_map(subscribers)
            if subscribers_info_map is None:
                raise XMLParserException("Parsing failed.. unable to process subscribers information")

            self.subscribers_details_map[subscribers_info_counter] = subscribers_info_map
            subscribers_info_counter += 1

        self.logger.debug("Parsing completed for XML file: [ %s ]." % filename)


    def get_subscribers_data_map(self):
        return self.subscribers_details_map

    # -------------------------------------> Private Members

    def __generate_subscriber_info_map(self, subscribers):
        return self.__generate_info_map(subscribers)

    def __generate_info_map(self, subscribers, is_recursive = True):

        # Get "type" attribute
        subscribers_type = self.__get_attribute_value(subscribers, XMLConstants.TYPE_TAG)
        if (subscribers_type is None or subscribers_type.__len__()) < 1:
            raise XMLParserException("Parsing failed... Empty [ %s ]. Some value is expected." % XMLConstants.TYPE_TAG)

        # Get "format" attribute
        subscribers_format = self.__get_attribute_value(subscribers, XMLConstants.FORMAT_TAG)
        if (subscribers_format is None or subscribers_format.__len__()) < 1:
            raise XMLParserException("Parsing failed... Empty [ %s ]. Some value is expected." % XMLConstants.FORMAT_TAG)

        # Get "value" attribute
        subscribers_value = self.__get_attribute_value(subscribers, XMLConstants.VALUE_TAG)
        if (subscribers_value is None or subscribers_value.__len__()) < 1:
            raise XMLParserException("Parsing failed... Empty [ %s ]. Some value is expected." % XMLConstants.VALUE_TAG)

        # Get place holder map
        max_possible_combinations, subscriber_place_holder_map = self.__get_place_holder_map(
            subscribers_format, subscribers_value)

        # Get "limit" attribute - this is optional field
        subscribers_limit = self.__get_attribute_value(subscribers, XMLConstants.LIMIT_TAG)
        
        if subscribers_limit is not None:
            if subscribers_limit.__len__() < 1:
                raise XMLParserException("Parsing failed... Empty [ %s ]. Some value is expected." % XMLConstants.LIMIT_TAG)
            # Check for negative values
            try:
                subscribers_limit_int_value = int(subscribers_limit)
                if subscribers_limit_int_value < 0:
                    raise XMLParserException("Parsing failed... limit cannot be negative.")
                elif subscribers_limit_int_value > max_possible_combinations:
                    self.logger.warn("Found limit: [ %d ] greater than max possible combinations: [ %d ] for subscriber type: [ %s ]. Using max possible value..." % (subscribers_limit_int_value,
                                                                                                                                                                      max_possible_combinations,
                                                                                                                                                                      subscribers_type))
                    subscribers_limit = max_possible_combinations.__str__()
            except ValueError:
                raise XMLParserException(
                    "Parsing failed... limit should be a number.")
        else:
            subscribers_limit = max_possible_combinations.__str__()

        self.logger.debug("Generated subscriber info data for - Type [%s] Format: [ %s ], Value: [ %s ], Limit: [ %s ], Placeholder Map: [%s]." % (subscribers_type,
                                                                                                                                                   subscribers_format,
                                                                                                                                                   subscribers_value,
                                                                                                                                                   subscribers_limit,
                                                                                                                                                   subscriber_place_holder_map))
        # Check for dependent entries; mainly used for allowed and blocked and
        # scenarios
        subscriber_dependent_info_list = []
        if is_recursive:
            for subscriber_dep in subscribers.findall(XMLConstants.CALLING_SUBSCRIBER_TAG):

                # Recursive call
                subscriber_dependent_info_map = self.__generate_info_map(subscriber_dep, False)
                subscriber_dependent_info_list.append(subscriber_dependent_info_map)
            # ----- End of For-Loop
        else:

            # Checking for second level hierarchy
            subscribers_dep_list = subscribers.findall(XMLConstants.CALLING_SUBSCRIBER_TAG)
            if (subscribers_dep_list is not None and subscribers_dep_list.__len__() > 1):
                raise XMLParserException("Parsing failed... only single level hierarchy allowed.")

            # Check for Inherit tag
            inherit_tag_list = re.findall(XMLConstants.INHERIT_TAG, subscribers_format.__str__())            
            if subscriber_dependent_info_list.__len__() < 1 and is_recursive:
                if inherit_tag_list.__len__() > 0:
                    raise XMLParserException("Parsing failed... INHERIT tag not allowed.")
            else:
                if inherit_tag_list.__len__() > 1:
                    raise XMLParserException("Parsing failed... There can be single INHERIT tag allowed for dependent")
                
        # Creating subscriber info map for corresponding child
        subscriber_info_map = {XMLConstants.XML_NODE_NAME_TAG: subscribers.tag,
                               XMLConstants.TYPE_TAG: subscribers_type,
                               XMLConstants.FORMAT_TAG: subscribers_format,
                               XMLConstants.PLACE_HOLDER_TAG: subscriber_place_holder_map,
                               XMLConstants.LIMIT_TAG: subscribers_limit,
                               XMLConstants.SUBSCRIBER_DEPENDENTS: subscriber_dependent_info_list}

        return subscriber_info_map

    def __get_place_holder_map(self, subscribers_format, subscribers_value):

        msg = ""
        status = False
        place_holder_map = OrderedDict()
        max_possible_combinations = 1

        # This loop will run only once
        for _ in [1]:

            place_holders_present = False
            place_holder_list = re.findall(
                XMLConstants.PLACE_HOLDER_SEARCH_REGEX, subscribers_format.__str__())
            if place_holder_list.__len__() > 0:
                place_holders_present = True

            # Check if place holders are present in the format tag? If Yes, then continue parsing else
            # Just return an empty place holder map
            if place_holders_present:

                # Checking whether the value for place holder tag exists in the
                # scenario value
                place_holder_range_list = subscribers_value.__str__().split(
                    XMLConstants.SEMI_COLON_TAG)
                if place_holder_range_list.__len__() < 1:
                    msg = "Parsing Failed... No place holder tags exist in the scenario value field."
                    break

                # Till here we know that place holders are present ..
                are_valid_place_holders = True
                for place_holder_tag_value_pair in place_holder_range_list:

                    # Extract place holders and value pairs ...
                    place_holder_tag_and_value = place_holder_tag_value_pair.__str__().split(
                        XMLConstants.EQUALS_TAG)

                    # Check if the place holders are valid ?
                    place_holder_tag = place_holder_tag_and_value.__getitem__(
                        0).strip()
                    if not place_holder_list.__contains__(place_holder_tag):
                        msg = "Parsing Failed... Invalid place holder found: [ %s ], where scenario format is [ %s ]." % (
                            subscribers_format, place_holder_tag)
                        are_valid_place_holders = False
                        break

                    # This is the case when no value exist for the particular
                    # tag
                    if place_holder_tag_and_value.__len__() < 2:
                        msg = "Parsing Failed... No value exist for the place holder: [ %s ] in scenario value field." % place_holder_tag
                        are_valid_place_holders = False
                        break

                    place_holder_value = place_holder_tag_and_value.__getitem__(
                        1)
                    place_holder_start_end_range = place_holder_value.strip().split(
                        XMLConstants.COLON_TAG)

                    # Extract start and end ranges ...
                    start_range = ""
                    end_range = ""
                    if place_holder_start_end_range.__len__() < 2:
                        self.logger.debug(
                            "No range delimiter specified, it will use the current value.")
                        start_range = end_range = place_holder_start_end_range.__getitem__(
                            0)
                    elif place_holder_start_end_range.__len__() == 2:
                        start_range = place_holder_start_end_range.__getitem__(
                            0)
                        end_range = place_holder_start_end_range.__getitem__(1)
                    else:
                        msg = "Parsing Failed... Invalid range values."
                        are_valid_place_holders = False
                        break

                    # Checking if start and end range are valid
                    try:

                        start_range_int_value = int(start_range)
                        end_range_int_value = int(end_range)

                        if start_range_int_value < 0:
                            msg = "Parsing Failed... start range value: [ %d ] cannot be negative." % start_range_int_value
                            are_valid_place_holders = False
                            break

                        if end_range_int_value < 0:
                            msg = "Parsing Failed... end range value: [ %d ] cannot be negative." % end_range_int_value
                            are_valid_place_holders = False
                            break

                        if start_range_int_value > end_range_int_value:
                            msg = "Parsing Failed... start range value: [ %d ] cannot be greater than end range value: [ %d ]." % (
                                start_range_int_value, end_range_int_value)
                            are_valid_place_holders = False
                            break

                        # Calculating max possible combinations
                        max_possible_combinations *= (
                            end_range_int_value - start_range_int_value + 1)

                    except ValueError as err:
                        msg = err
                        are_valid_place_holders = False
                        break

                    # Calculate place holder max length; it will depend on the end range, as it is the max range till
                    # where the number will go ...
                    place_holder_max_length = end_range.__len__()

                    # Ready to add tuple
                    place_holder_map[(place_holder_tag, place_holder_max_length)] = (start_range_int_value,
                                                                                     end_range_int_value)
                # ------> Inner loop ends here ....

                if not are_valid_place_holders:
                    # Failure case
                    break

            # Successfully handled data for parsing
            status = True

        # ------> Outer loop ends here ....
        if not status:
            raise XMLParserException(msg)

        return max_possible_combinations, place_holder_map

    def __get_attribute_value(self, node, tag_name):
        tag_value = node.get(tag_name)
        if not tag_value is None:
            tag_value = tag_value.__str__().strip()
        return tag_value


    @staticmethod
    def get_dummy_subscriber_data_map(subscriber_type = None):
        info_map = {XMLConstants.XML_NODE_NAME_TAG: XMLConstants.DELETE_NODE_NAME_TAG,
                               XMLConstants.TYPE_TAG: subscriber_type,
                               XMLConstants.FORMAT_TAG: None,
                               XMLConstants.PLACE_HOLDER_TAG: None,
                               XMLConstants.LIMIT_TAG: None,
                               XMLConstants.SUBSCRIBER_DEPENDENTS: None}
        subscriber_data_map = {}
        subscriber_data_map[0] =  info_map
        return subscriber_data_map

# ---------------------> XML Parser Exceptions

class XMLParserException(Exception):
    pass
