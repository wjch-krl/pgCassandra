from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

class CassandraFDW(ForeignDataWrapper):
    IDX_QUERY_COST = 1000
    CLUSTERING_KEY_QUERY_COST = 100
    PARTITION_KEY_QUERY_COST = 10
    PRIMARY_KEY_QUERY_COST = 1

    def __init__(self, options, columns):
        super(CassandraFDW, self).__init__(options, columns)
        # get options
        if "hosts" not in options:
            log_to_postgres("The hosts parameter is needed, setting to localhost.", WARNING)
        hosts = options.get("hosts", "localhost").split(",");
        if "port" not in options:
            log_to_postgres("The port parameter is needed, setting to  9042.", WARNING)
        self.port = options.get("port", "9042")
        if (("keyspace" not in options) or ("columnfamily" not in options)) and ("query" not in options):
            log_to_postgres("Either query or columnfamily parameter is required.", ERROR)
        self.columnfamily = options.get("columnfamily", None)
        self.keyspace = options.get("keyspace", None)
        self.query = options.get("query", None)
        self.limit = options.get("limit", None)
        timeout =  options.get("timeout", None)
        
        # Cassandra connection init
        self.cluster =  Cluster(hosts)
        self.session = self.cluster.connect()
        if (timeout)
            self.session.default_timeout = timeout
        # Get querable columns
        tableInfoQuery = "select column_name,type,index_type from system.schema_columns" +
            " where keyspace_name = '{0}' and columnfamily_name = '{1}'".format(keyspace,columnfamily)
        result = self.session.execute(statement)
        self.queryableColumns = {}
        for row in result:
            columnName = row["column_name"]
            columnKeyType = row["type"]
            columnIdxType = row["index_type"]
            if (columnKeyType == "regular") and (columnIdxType == None):
                continue
            self.queryableColumns[columnName] = keyTypeToCost(columnKeyType,columnIdxType)    
        
    def keyTypeToCost(columnKeyType,columnIdxType):
        if (columnKeyType == "clustering_key"):
            return CLUSTERING_KEY_QUERY_COST
        if (columnKeyType == "partition_key"):
            return PARTITION_KEY_QUERY_COST
        return IDX_QUERY_COST
        
    def execute(self, quals, columns):
        if (self.query):
            statement = self.query
        else:
            statement = "SELECT {0} FROM {1}".format(",".join(columns), self.columnfamily);
        # TODO don't query when clustering key is queried and partion key isn't
        isWhere = None
        for qual in quals:
            if (qual.operator == "="):
                if (qual.field_name in self.queryableColumns):
                    if isWhere:
                        statement += " AND {0} = {1} ".format(qual.field_name ,qual.value)
                    else:
                        statement += " WHERE {0} = {1} ".format(qual.field_name ,qual.value)
                        isWhere = 1
        if (self.limit):
            staement += " limit {0}".format(limit);
        log_to_postgres("CQL query: {1}".format(unicode(statement)), WARNING)
        
        result = self.session.execute(statement)
        for row in result:
            line = {}
            idx = 0
            for column_name in columns:
                line[column_name] = row[idx++]
            yield line
            
    # def qualValueToString(qual,columns):
    #     if qual.     
            
    def get_path_keys(self):
        return [(obj.key, obj.value) for obj in self.queryableColumns]
        
        
