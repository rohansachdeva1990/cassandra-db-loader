import ConfigParser
import multiprocessing

from utils import CommonUtils

class ConfigurationReader(object):
    
    # Properties tags
    LOGGER_SECTION_TAG = "LOGGER"
    LOGGER_NAME_TAG = "loggerName"
    LOGGER_FILE_PATH_TAG = "loggerFilePath"
    LOGGER_FORMAT_TAG = "loggerFormat"
    LOGGER_MAX_BYTES_TAG = "loggerMaxBytes"
    LOGGER_BACKUP_COUNT_TAG = "loggerBackupCount"
    LOGGER_LEVEL_TAG = "loggerLevel"

    DB_SECTION_TAG = "DB"
    DB_CLUSTER_CONNECTION_POINT_TAG = "dbClusterContactPoints"
    DB_CLUSTER_PROTOCOL_VERSION_TAG = "dbClusterProtocolVersion"
    DB_BATCH_SIZE_TAG = "dbBatchSize"
    DB_NO_OF_CONNECTION_POOLS_TAG = "dbNoConnectionOfPools"
    DB_NO_OF_SESSIONS_TAG = "dbNoOfSessions"
    DB_SESSION_CONCURRENCY_TAG = "dbSessionConcurrency"
    DB_DEBUG_STATS_ENABLED_TAG = "dbDebugStatsEnabled"
        
    # Default properties values for logging
    DEFAULT_LOGGER_NAME_VALUE = "SkyDBLoader"
    DEFAULT_LOGGER_FILE_PATH_VALUE = "/../log/DatabaseLoader.log"
    DEFAULT_LOGGER_FORMAT_VALUE = "%(asctime)s - %(name)s - %(threadName)s - %(thread)d - %(lineno)d - %(levelname)s - %(message)s"
    DEFAULT_LOGGER_MAX_BYTES_ALLOWED_VALUE = 10485760
    DEFAULT_LOGGER_BACKUP_COUNT_VALUE = 10
    DEFAULT_LOGGER_LOG_LEVEL_VALUE = "DEBUG"

    # Default properties values for database
    DEFAULT_CLUSTER_CONNECTION_POINT_VALUE = "127.0.0.1"
    DEFAULT_DB_CLUSTER_PROTOCOL_VERSION_VALUE = 3
    DEFAULT_DB_BATCH_SIZE_VALUE = 200000
    DEFAULT_DB_NO_OF_CONNECTION_POOLS_VALUE = 2
    DEFAULT_DB_NO_OF_SESSIONS_VALUE = (multiprocessing.cpu_count() / 2)
    DEFAULT_DB_DEBUG_STATS_ENABLED_FLAG_VALUE = False
    DEFAULT_DB_SESSION_CONCURRENCY_VALUE = 100
    
    def __init__(self):
        self.config = ConfigParser.ConfigParser()
                
        # Add new properties here ...
        self.loggerName = ConfigurationReader.DEFAULT_LOGGER_NAME_VALUE
        self.loggerFilePath = ConfigurationReader.DEFAULT_LOGGER_FILE_PATH_VALUE
        self.loggerFormat = ConfigurationReader.DEFAULT_LOGGER_FORMAT_VALUE
        self.loggerMaxBytes = ConfigurationReader.DEFAULT_LOGGER_MAX_BYTES_ALLOWED_VALUE
        self.loggerBackupCount = ConfigurationReader.DEFAULT_LOGGER_BACKUP_COUNT_VALUE
        self.loggerLevel = ConfigurationReader.DEFAULT_LOGGER_LOG_LEVEL_VALUE
        self.dbClusterConnectionPoint = ConfigurationReader.DEFAULT_CLUSTER_CONNECTION_POINT_VALUE
        self.dbClusterProtocolVersion = ConfigurationReader.DEFAULT_DB_CLUSTER_PROTOCOL_VERSION_VALUE
        self.dbNoOfSessions = ConfigurationReader.DEFAULT_DB_NO_OF_SESSIONS_VALUE
        self.dbBatchSize = ConfigurationReader.DEFAULT_DB_BATCH_SIZE_VALUE 
        self.dbNoOfConnectionPools = ConfigurationReader.DEFAULT_DB_NO_OF_CONNECTION_POOLS_VALUE
        self.dbSessionConcurrency = ConfigurationReader.DEFAULT_DB_SESSION_CONCURRENCY_VALUE
        self.dbDebugStatsEnabled = ConfigurationReader.DEFAULT_DB_DEBUG_STATS_ENABLED_FLAG_VALUE
        
    def read(self, config_file_name):
        try:
            # Read and parse a file
            self.config.read(config_file_name)
        except ValueError:
            print("Unable to open configuration file: [ %s ]. Exiting Database Loader..." % config_file_name)
            return False

        # Get the configuration INI file.
        sections = self.config.sections()
        for section in sections:

            # Check and update logger properties
            if section == self.LOGGER_SECTION_TAG:

                # Get Logger File Path
                if self.config.has_option(section, self.LOGGER_FILE_PATH_TAG):
                    self.loggerFilePath = self.config.get(section, self.LOGGER_FILE_PATH_TAG)

                # Get Logger Max Bytes
                if self.config.has_option(section, self.LOGGER_MAX_BYTES_TAG):
                    self.loggerMaxBytes = self.config.get(section, self.LOGGER_MAX_BYTES_TAG)

                # Get Logger Backup Count
                if self.config.has_option(section, self.LOGGER_BACKUP_COUNT_TAG):
                    self.loggerBackupCount = self.config.get(section, self.LOGGER_BACKUP_COUNT_TAG)

                # Get Logger Level
                if self.config.has_option(section, self.LOGGER_LEVEL_TAG):
                    self.loggerLevel = self.config.get(section, self.LOGGER_LEVEL_TAG)

            # Check and update data base properties
            if section == self.DB_SECTION_TAG:

                # Get DB Cluster Connection Points
                if self.config.has_option(section, self.DB_CLUSTER_CONNECTION_POINT_TAG):
                    self.dbClusterConnectionPoint = self.config.get(section,self.DB_CLUSTER_CONNECTION_POINT_TAG)

                # Get DB Cluster Protocol Version
                if self.config.has_option(section, self.DB_CLUSTER_PROTOCOL_VERSION_TAG):
                    self.dbClusterProtocolVersion = self.config.get( section, self.DB_CLUSTER_PROTOCOL_VERSION_TAG)

                # Get DB Batch Size
                if self.config.has_option(section, self.DB_BATCH_SIZE_TAG):
                    batchSize = int(self.config.get(section, self.DB_BATCH_SIZE_TAG))
                    if not CommonUtils.represent_int(batchSize):
                        print("Invalid property [ %s ] value: [ %s ]. Exiting Database Loader..." % (self.DB_BATCH_SIZE_TAG, batchSize))
                        return False
                    batchSizeIntValue = int(batchSize)
                    if batchSizeIntValue > int(ConfigurationReader.DEFAULT_DB_BATCH_SIZE_VALUE):
                        print ("Database batch size cannot be greater than default value: [ %s ]. Using default value..." % ConfigurationReader.DEFAULT_DB_BATCH_SIZE_VALUE)
                        self.dbBatchSize = ConfigurationReader.DEFAULT_DB_BATCH_SIZE_VALUE
                    else:
                        self.dbBatchSize = batchSizeIntValue
        
                # Get DB No Of Pools
                if self.config.has_option(section, self.DB_NO_OF_CONNECTION_POOLS_TAG):
                    noOfConnectionPools = int(self.config.get(section, self.DB_NO_OF_CONNECTION_POOLS_TAG))
                    if not CommonUtils.represent_int(noOfConnectionPools):
                        print("Invalid property [ %s ] value: [ %s ]. Exiting Database Loader..." % (self.DB_NO_OF_CONNECTION_POOLS_TAG, noOfConnectionPools))
                        return False
                    
                    noOfConnectionPoolsIntValue = int(noOfConnectionPools)
                    if noOfConnectionPoolsIntValue > int(ConfigurationReader.DEFAULT_DB_NO_OF_CONNECTION_POOLS_VALUE):
                        print ("No of connection pools cannot be greater than default value: [ %s ]. Using default value..." % ConfigurationReader.DEFAULT_DB_NO_OF_CONNECTION_POOLS_VALUE)
                        self.dbNoOfConnectionPools = ConfigurationReader.DEFAULT_DB_NO_OF_CONNECTION_POOLS_VALUE
                    else:
                        self.dbNoOfConnectionPools = noOfConnectionPoolsIntValue
                                            
                # Get DB No Of Sessions
                permissible_cpu_count = ConfigurationReader.DEFAULT_DB_NO_OF_SESSIONS_VALUE
                if permissible_cpu_count <= 1:
                    permissible_cpu_count = multiprocessing.cpu_count()
                    print ("Your system supports [ %d ] cores. Using current number of sessions. " % permissible_cpu_count)
                    self.dbNoOfSessions = permissible_cpu_count
                else:
                    if self.config.has_option(section, self.DB_NO_OF_SESSIONS_TAG):
                        dBNoOfSessions =  self.config.get(section, self.DB_NO_OF_SESSIONS_TAG)
                        if not CommonUtils.represent_int(dBNoOfSessions):
                            print("Invalid property [ %s ] value: [ %s ]. Exiting Database Loader..." % (self.DB_NO_OF_SESSIONS_TAG, dBNoOfSessions))
                            return False
                    dBNoOfSessionsIntValue = int(dBNoOfSessions)
                    if permissible_cpu_count < dBNoOfSessionsIntValue:
                        print ("No of sessions cannot be greater than the default value: [ %s ]. Using default value..." % ConfigurationReader.DEFAULT_DB_NO_OF_SESSIONS_VALUE)
                        self.dbNoOfSessions = ConfigurationReader.DEFAULT_DB_NO_OF_SESSIONS_VALUE
                    else:
                        self.dbNoOfSessions = dBNoOfSessionsIntValue
                        
                # Get Session Concurrency
                if self.config.has_option(section, self.DB_SESSION_CONCURRENCY_TAG):
                    dbSessionConcurrency = int(self.config.get(section, self.DB_SESSION_CONCURRENCY_TAG))
                    if not CommonUtils.represent_int(dbSessionConcurrency):
                        print("Invalid property [ %s ] value: [ %s ]. Exiting Database Loader..." % (self.DB_SESSION_CONCURRENCY_TAG, dbSessionConcurrency))
                        return False
                    
                    dbSessionConcurrencyIntValue = int(dbSessionConcurrency)
                    if dbSessionConcurrencyIntValue > int(ConfigurationReader.DEFAULT_DB_SESSION_CONCURRENCY_VALUE):
                        print ("No of connection pools cannot be greater than default value: [ %s ]. Using default value..." % ConfigurationReader.DEFAULT_DB_SESSION_CONCURRENCY_VALUE)
                        self.dbSessionConcurrency = ConfigurationReader.DEFAULT_DB_SESSION_CONCURRENCY_VALUE
                    else:
                        self.dbSessionConcurrency = dbSessionConcurrencyIntValue
                
                # Check if DB Debug Stats Enabled
                if self.config.has_option(section, self.DB_DEBUG_STATS_ENABLED_TAG):
                    dbDebugStatsEnabled = self.config.get( section, self.DB_DEBUG_STATS_ENABLED_TAG)
                    if dbDebugStatsEnabled != "False":
                        self.dbDebugStatsEnabled = True
                        
        # Configuration read successfully...
        return True
    
    # Properties Getters
    def get_logger_name(self):
        return self.loggerName
    
    def get_logger_file_path(self):
        return self.loggerFilePath
    
    def get_logger_format(self):
        return self.loggerFormat
    
    def get_logger_max_bytes(self):
        return self.loggerMaxBytes
    
    def get_logger_backup_count(self):
        return self.loggerBackupCount
    
    def get_logger_level(self):
        return self.loggerLevel
    
    def get_db_cluster_connection_point(self):
        return self.dbClusterConnectionPoint
    
    def get_db_cluster_protocol_version(self):
        return self.dbClusterProtocolVersion
    
    def get_db_no_of_sessions(self):
        return self.dbNoOfSessions
    
    def get_db_batch_size(self):
        return self.dbBatchSize
    
    def get_db_no_of_connection_pools(self):
        return self.dbNoOfConnectionPools
    
    def get_db_session_concurrency(self):
        return self.dbSessionConcurrency
       
    def is_debug_stats_enabled(self):
        return self.dbDebugStatsEnabled