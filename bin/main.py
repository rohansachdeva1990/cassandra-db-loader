import os.path
import signal
import sys

# from bin.dbclient.cassandradb.cassandra_client import CassandraClient
# from database_loader import DatabaseLoader
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from bin import global_settings

from bin.database_loader import DatabaseLoader
from bin.dbclient.cassandradb.cassandra_client import CassandraClient


# Global Database loader object to trigger start and stop of the database loading process.
# Added this so that we can control it state from signal_handler
database_loader_obj = None 

def initialize_database_loader():
    # Registering signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    if not global_settings.initialize():
        return False
    
    global database_loader_obj
    database_loader_obj = DatabaseLoader()
  
    print "Database Loader Initialized..."
    return True

def start_database_loader():
    print "Database Loader Started..."
    print "Press Ctrl + C to stop the database loading process gracefully..."

    db_client = CassandraClient()
    status = database_loader_obj.start_population(global_settings.SUBSCRIBER_DATA_FILE, db_client)
        
    print "Database Loader Finished..."
    return status

def start_database_dependent_deleter(subscriber_type):
    print "Database Dependent Deleter started..."
    print "Press Ctrl + C to stop the process gracefully..."

    db_client = CassandraClient()
    status = database_loader_obj.start_deletion(db_client, subscriber_type)
        
    print "Finished removing the dependents..."
    return status

def stop_database_loader():
    database_loader_obj.stop()
    print "Database Loader Stopped gracefully..."

# Signal handler to gracefully shutdown the database loading process. It will continue
# processing the last batch it is currently executing.
def signal_handler(signal, frame):
    print "Please wait... Gracefully shutting down the database loading process."
    stop_database_loader()

def help_msg():
    print "\n********* Welcome to Database Loader *********"
    print "Usage: 1. sh run.sh -r or py %s -r for population" % (sys.argv[0])
    print "       2. sh run.sh -d [subscriber_type] or py %s -d [subscriber_type] for deletion" % (sys.argv[0])
    print "For help, type: %s -h"  % (sys.argv[0])
    
def main():
    
    if(len(sys.argv) < 2):
        help_msg()
        sys.exit()
      
    if(sys.argv[1] == "-h" or sys.argv[1] == "-help"):
        help_msg()
        sys.exit()
     
    status = initialize_database_loader()
    if status:
        if(sys.argv[1] == "-d"):
            if (sys.argv < 3):
                help_msg()
                sys.exit()
                 
            status = start_database_dependent_deleter(sys.argv[2].strip())
        else :
            status = start_database_loader()

# Starting point
if __name__ == '__main__':
    main()