""" 
    DB Client module.

    This is the interface module for Database client. In future, this interface 
    can be used as a blueprint for new Database type

"""
from abc import abstractmethod
import abc


class DbClient:

    __metaclass__ = abc.ABCMeta
    
    @abstractmethod
    def connect(self, query_manager):
        pass

    @abstractmethod
    def close(self):
        pass
    
    @abstractmethod
    def get_client_handle(self):
        pass

class DbClientException(Exception):
    pass
