from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

class CassandraFDW(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(CassandraFDW, self).__init__(options, columns)
        if 'hosts' not in options:
            log_to_postgres('The hosts parameter is required and the default is localhost.', WARNING)
        hosts = options.get("hosts", "localhost").split(",");
        
        if 'port' not in options:
            log_to_postgres('The host parameter is required and the default is 9042.', WARNING)
        self.port = options.get("port", "9042")
        
        if ('columnfamily'  not in options) and ('query' not in options):
            log_to_postgres('Either query or columnfamily parameter is required.', ERROR)
        self.columnfamily = options.get("columnfamily", None)
        self.query = options.get("query", None)
        queryableColumns = options.get("queryable_columns", None).split(",")
        
        self.columns = columns
        self.queryableColumns = queryableColumns
        self.cluster =  Cluster(hosts)
        self.session = self.cluster.connect()
        self.options = options
        
    def execute(self, quals, columns):
        if self.query:
            statement = self.query
        else:
            statement = "SELECT " + ",".join(columns) + " FROM " + self.columnfamily
        log_to_postgres('Query Filters:  %s' % quals, WARNING)
        
        isWhere = None
        for qual in quals:
            if qual.operator == "=":
                if qual.field_name in self.queryableColumns:
                    if isWhere:
                        statement += " and " + qual.field_name + ' = %s' % qual.value
                    else:
                        statement += " where " + qual.field_name + ' = %s' % qual.value
                        isWhere = 1
        
        staement = statement + " limit 30";
        log_to_postgres('CQL query: ' + unicode(statement), DEBUG)
        
        result = self.session.execute(statement)
        for row in result:
            line = {}
            idx = 0
            for column_name in columns:
                line[column_name] = row[idx]
                idx = idx + 1
            yield line