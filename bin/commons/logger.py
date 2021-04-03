import logging.handlers
import os
import sys

from bin import global_settings

class DBLoaderLogger(object):
    
    isLoggerInitialized = False
    handler = None

    @staticmethod
    def __initialize_logging():

        configuration_reader = global_settings.configuration_reader

        logfile = os.getcwd() + configuration_reader.get_logger_file_path()

        formatter = logging.Formatter(configuration_reader.get_logger_format())

        # Configure default logger handler
        try:
            DBLoaderLogger.handler = logging.handlers.RotatingFileHandler(logfile,
                                                                          maxBytes=configuration_reader.get_logger_max_bytes(),
                                                                          backupCount=configuration_reader.get_logger_backup_count())
        except ValueError:
            print("Error opening log file: [ %s ]. Exiting Database Loader..." % logfile)
            return False

        DBLoaderLogger.handler.setFormatter(formatter)

        return True

    @staticmethod
    def __get_logging_level(level):
        if level == "DEBUG":
            return logging.DEBUG
        elif level == "INFO":
            return logging.INFO
        elif level == "WARN":
            return logging.WARN
        elif level == "ERROR":
            return logging.ERROR
        elif level == "CRITICAL":
            return logging.CRITICAL
        else:
            print ("Invalid logging level found : [ %s ], using default logging level: DEBUG." % level)
            return logging.DEBUG

    @staticmethod
    def get_instance(name):
        logger = DBLoaderLogger.__get_instance(name)
        if logger is None:
            print("Failed to create logger for : [ %s ]. Exiting Database Loader..." % name)
            sys.exit(1)
            
        return logger
            
    @staticmethod
    def __get_instance(name):   

        # This is done so that our logger is only initialized once and we can
        # create logger as per our module.
        if not DBLoaderLogger.isLoggerInitialized:
            if DBLoaderLogger.__initialize_logging():
                DBLoaderLogger.isLoggerInitialized = True
            else:
                return None
            
        logger = logging.getLogger(name)
        logger.addHandler(DBLoaderLogger.handler)
        logger.setLevel(DBLoaderLogger.__get_logging_level(global_settings.configuration_reader.get_logger_level()))
        
        return logger