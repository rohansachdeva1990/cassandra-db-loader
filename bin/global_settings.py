from multiprocessing.managers import BaseManager
import os

from bin.commons.configuration_reader import ConfigurationReader

script_path = os.getcwd()
SUBSCRIBER_DATA_FILE = script_path + "/../config/subscriber_data.xml"
CONFIG_INI_FILE = script_path + "/../config/config.ini"
TMP_DIR_PATH = script_path + "/../tmp"


configuration_reader = None
setting_initialized = False

# To be done only once
def initialize():
    
    global setting_initialized
    if not setting_initialized:
        if not os.path.isfile(CONFIG_INI_FILE):
            print("Invalid database loader configuration file found: [ %s ]. Exiting Database Loader..." % CONFIG_INI_FILE)
            return False
        
        if not os.path.isfile(SUBSCRIBER_DATA_FILE):
            print("Invalid database loader subscriber data file found: [ %s ]. Exiting Database Loader..." % SUBSCRIBER_DATA_FILE)
            return False
        
        if not os.path.exists(TMP_DIR_PATH):
            os.makedirs(TMP_DIR_PATH)
        
        # So that property values are available during multiple process
        MyManager.register('ConfigurationReader', ConfigurationReader)
        manager = Manager()
        
        # Read configuration file for setting up logger, database etc.
        global configuration_reader
        configuration_reader = manager.ConfigurationReader()
        if not configuration_reader.read(CONFIG_INI_FILE):
            print("Failed to read configuration file: [ %s ]. Exiting Database Loader..." % CONFIG_INI_FILE)
            return False
        
        setting_initialized = True
        
    return True

class MyManager(BaseManager): pass

def Manager():
    manager = MyManager()
    manager.start()
    return manager 