from bin.commons.xml_parser import XMLConstants


class CassandraQueryManager:
    def __init__(self, subscriber_data_map):
        self.subscriber_data_map = subscriber_data_map
        
    # Throws exception
    def get_instance(self, session):
        cassandra_query_manager = CassandraPreparedQueryStore(session, self.subscriber_data_map)
        cassandra_query_manager.prepare()
        return cassandra_query_manager

class CassandraPreparedQueryStore:
    
    def __init__(self, session, subscribers_data_map):
        self.session = session
        self.subscribers_data_map = subscribers_data_map 
        self.prepare_stmt_map = {}
        
    def prepare(self):
        # Preparing query map; for easy retrieval at the time of tuple preparation.
        self.__prepare(self.subscribers_data_map)
        
    def __prepare(self, subscribers_data_map):
        for subscribers_group_index in subscribers_data_map:
            subscribers_info_map = self.subscribers_data_map[subscribers_group_index]
            
            query_map = {}
            self.__populate_query_map(subscribers_info_map, query_map)
            self.prepare_stmt_map[subscribers_group_index] = query_map
        
    def __populate_query_map(self, subscribers_info_map, query_map):        
        # Recursively, populate the query map according to subscriber type.
        node_name = subscribers_info_map.__getitem__(XMLConstants.XML_NODE_NAME_TAG)
        subscribers_type = subscribers_info_map.__getitem__(XMLConstants.TYPE_TAG)
        query_map[(subscribers_type, node_name)] = self.__get_query(subscribers_type, node_name)


        subscriber_dependent_list = subscribers_info_map.__getitem__(XMLConstants.SUBSCRIBER_DEPENDENTS)
        if (subscriber_dependent_list is not None and subscriber_dependent_list.__len__() > 0):
            for sd_info_map in subscriber_dependent_list:
                # Recursive call
                self.__populate_query_map(sd_info_map, query_map)
                
    def get_prepared_statement(self, subscribers_group_index, subscribers_type, subscribers_tag_name):
        # Fetch the valid prepare statement according to input.
        statement = None
        query_map = self.prepare_stmt_map.__getitem__(subscribers_group_index)
        if (query_map is not None and query_map.__len__() > 0):
            statement = query_map.__getitem__((subscribers_type, subscribers_tag_name))
        
        return statement

    def __get_query(self, subscribers_type, node_name):
        # This function, returns prepared query, as per the subscriber type
        if node_name == XMLConstants.SUBSCRIBERS_TAG:
            
            if subscribers_type == XMLConstants.SCENARIO_TYPE_ALLOWED:
                return self.session.prepare(CassandraQueries.INSERT_UPS_ALLOWED_SCENARIO_STMT)

            elif subscribers_type == XMLConstants.SCENARIO_TYPE_BLOCKED:
                return self.session.prepare(CassandraQueries.INSERT_UPS_BLOCKED_SCENARIO_STMT)

            elif subscribers_type == XMLConstants.SCENARIO_TYPE_SCREENING_ALLOWED:
                return self.session.prepare(CassandraQueries.INSERT_UPS_SCREENING_ALLOWED_SCENARIO_STMT)

            elif subscribers_type == XMLConstants.SCENARIO_TYPE_SCREENING_BLOCKED:
                return self.session.prepare(CassandraQueries.INSERT_UPS_SCREENING_BLOCKED_SCENARIO_STMT)
            else:
                raise CassandraQueryManagerException("Failed to prepare cassandra query statements... Invalid subscriber type: [ %s ] for node_name: [ %s ]" % (subscribers_type,
                                                                                                                                                                XMLConstants.SUBSCRIBERS_TAG))             
        elif node_name == XMLConstants.CALLING_SUBSCRIBER_TAG:
 
            if subscribers_type == XMLConstants.SCENARIO_TYPE_ALLOWED:
                return self.session.prepare(CassandraQueries.INSERT_UICBT_ALLOWED_SCENARIO_STMT)

            elif subscribers_type == XMLConstants.SCENARIO_TYPE_BLOCKED:
                return self.session.prepare(CassandraQueries.INSERT_UICBT_BLOCKED_SCENARIO_STMT)
            else:
                raise CassandraQueryManagerException("Failed to prepare cassandra query statements... Invalid subscriber type: [ %s ] for node_name: [ %s ]" % (subscribers_type,
                                                                                                                                                                XMLConstants.CALLING_SUBSCRIBER_TAG))
        elif node_name == XMLConstants.DELETE_NODE_NAME_TAG:
            return self.session.prepare(CassandraQueries.DELETE_UICBT_STMT)
    
        else:
            raise CassandraQueryManagerException("Failed to prepare cassandra query statements... Invalid node name: [ %s ]" % node_name)
            
# Exceptions
class CassandraQueryManagerException(Exception):
    pass


class CassandraQueries:
    
    # For PSUSER_DATA
    PSUSER_DATA_SCHEMA = "psuser_data"
    CREATE_PSUSER_DATA_SCHEMA_STMT = "CREATE KEYSPACE IF NOT EXISTS psuser_data WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1} AND DURABLE_WRITES = true"
    USE_PSUSER_DATA_SCHEMA_STMT = "USE psuser_data"
    DROP_PSUSER_DATA_SCHEMA_STMT = "DROP KEYSPACE IF EXISTS psuser_data"
    
    # For USER_PROFILE_S
    CREATE_UPS_TABLE_STMT = "CREATE TABLE IF NOT EXISTS psuser_data.user_profile_s ( usernumber bigint PRIMARY KEY, anonymousscreeningenabled boolean, customerkey uuid, enabled boolean, hasvoicemail boolean, modifiedtime timestamp, operatorid int, screeningenabled boolean) WITH compaction = { 'class': 'LeveledCompactionStrategy' } AND compression = { 'crc_check_chance' : 1.0, 'sstable_compression' : 'SnappyCompressor'}"
    DROP_UPS_TABLE_STMT = "DROP TABLE IF EXISTS user_profile_s"
    INSERT_UPS_ALLOWED_SCENARIO_STMT = "INSERT INTO psuser_data.user_profile_s( usernumber, anonymousscreeningenabled, customerkey, enabled, hasvoicemail, modifiedtime, operatorid, screeningenabled) values ( ?, TRUE, ?, TRUE, TRUE, ?, null, TRUE)"
    INSERT_UPS_BLOCKED_SCENARIO_STMT = "INSERT INTO psuser_data.user_profile_s( usernumber, anonymousscreeningenabled, customerkey, enabled, hasvoicemail, modifiedtime, operatorid, screeningenabled) values ( ?, TRUE, ?, TRUE, TRUE, ?, null, TRUE)"
    INSERT_UPS_SCREENING_ALLOWED_SCENARIO_STMT = "INSERT INTO psuser_data.user_profile_s( usernumber, anonymousscreeningenabled, customerkey, enabled, hasvoicemail, modifiedtime, operatorid, screeningenabled) values ( ?, TRUE, ?, TRUE, TRUE, ?, null, TRUE)"
    INSERT_UPS_SCREENING_BLOCKED_SCENARIO_STMT = "INSERT INTO psuser_data.user_profile_s( usernumber, anonymousscreeningenabled, customerkey, enabled, hasvoicemail, modifiedtime, operatorid, screeningenabled) values ( ?, TRUE, ?, TRUE, TRUE, ?, null, TRUE)"

    # For USER_IDENTIFIED_CALLER_BY_TELEPHONE
    CREATE_UICBT_TABLE_STMT = "CREATE TABLE psuser_data.user_identified_caller_by_telephone (c uuid, f bigint, i boolean, bl boolean, d boolean, t timestamp, PRIMARY KEY (c, f, i)) WITH compaction = { 'class': 'LeveledCompactionStrategy' } AND compression = { 'crc_check_chance' : 1.0, 'sstable_compression' : 'SnappyCompressor' }"
    DROP_UICBT_TABLE_STMT = " DROP TABLE IF EXISTS user_identified_caller_by_telephone"
    INSERT_UICBT_ALLOWED_SCENARIO_STMT = "INSERT INTO psuser_data.user_identified_caller_by_telephone ( c, f, i, bl, d, t) VALUES (?, ?, FALSE, FALSE, null,  ?)"
    INSERT_UICBT_BLOCKED_SCENARIO_STMT = "INSERT INTO psuser_data.user_identified_caller_by_telephone ( c, f, i, bl, d, t) VALUES (?, ?, FALSE, TRUE, null, ?)"
    INSERT_UICBT_SCREENING_ALLOWED_SCENARIO_STMT = "INSERT INTO psuser_data.user_identified_caller_by_telephone ( c, f, i, bl, d, t) VALUES (?, ?, FALSE, FALSE, null, ?)"
    INSERT_UICBT_SCREENING_BLOCKED_SCENARIO_STMT = "INSERT INTO psuser_data.user_identified_caller_by_telephone ( c, f, i, bl, d, t) VALUES (?, ?, FALSE, TRUE, null, ?)"
    DELETE_UICBT_STMT = "DELETE FROM psuser_data.user_identified_caller_by_telephone where c = ?"
    
    # For Debug Statistics
    DB_LOADER_DEBUG_STATS_SCHEMA = "db_loader_debug_stats"
    CREATE_DB_LOADER_DEBUG_STATS_SCHEMA_STMT = "CREATE KEYSPACE IF NOT EXISTS db_loader_debug_stats WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1} AND DURABLE_WRITES = true"
    USE_DB_LOADER_DEBUG_STATS_SCHEMA_STMT = "USE db_loader_debug_stats"
    DROP_DB_LOADER_DEBUG_STATS_SCHEMA_STMT = "DROP KEYSPACE IF EXISTS db_loader_debug_stats"
    CREATE_DEBUG_STATS_TABLE_STMT = "CREATE TABLE IF NOT EXISTS db_loader_debug_stats.db_loader_stats ( debug_index int PRIMARY KEY, counter_value counter) WITH compaction = { 'class': 'LeveledCompactionStrategy' } AND compression = { 'crc_check_chance' : 1.0, 'sstable_compression' : 'SnappyCompressor'}"
    DROP_DEBUG_STATS_TABLE_STMT = "DROP TABLE IF EXISTS db_loader_stats"
    UPDATE_DEBUG_STATS_TABLE_STMT = "UPDATE db_loader_debug_stats.db_loader_stats SET counter_value = counter_value + ? WHERE debug_index=1"
    