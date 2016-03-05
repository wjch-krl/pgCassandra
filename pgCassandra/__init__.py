from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO
from cassandra.cluster import Cluster
from collections import defaultdict
from decimal import Decimal
import math

IDX_QUERY_COST = 1000
CLUSTERING_KEY_QUERY_COST = 100
PARTITION_KEY_QUERY_COST = 1
PRIMARY_KEY_QUERY_COST = 1


def key_cost(self, column_key_type, column_idx_type):
    if column_key_type == "clustering_key":
        return self.CLUSTERING_KEY_QUERY_COST
    if column_key_type == "partition_key":
        return self.PARTITION_KEY_QUERY_COST
    return self.IDX_QUERY_COST


def format_qual_value(qual):
    if type(qual.value) in (unicode, str):
        return u"'{0}'".format(qual.value)
    return u"{0}".format(qual.value)


class CassandraFDW(ForeignDataWrapper):
    def __init__(self, options, columns):
        
        def init_connection():
            self.cluster = Cluster(self.hosts)
            self.session = self.cluster.connect()
            if self.timeout:
                self.session.default_timeout = self.timeout
        
        def get_index_info():
            version = get_version()
            query = table_info_query(version)
            result = self.session.execute(query, [self.keyspace, self.columnfamily])
            self.queryableColumns = {}
            for row in result:
                column_name = row[0]
                column_key_type = row[1]
                column_idx_type = row[2]
                if (column_key_type == "regular") and (column_idx_type is None):
                    continue
                self.queryableColumns[column_name] = self.key_cost(column_key_type, column_idx_type)
        
        def get_version():
            version_string, = self.session.execute("select release_version from system.local where key = 'local'")[0]
            groups = version_string.split('.')
            version = float('_'.join(groups[:2]))
            return version
        
        def table_info_query(version):
            if version < 3:
                table_name = "system.schema_columns"
            else:
                table_name = "system_schema.columns"
            query = "select column_name,type,index_type from {0} " \
                "where keyspace_name = %s and columnfamily_name = %s".format(table_name)
            return query
            
            def get_options():
            if "hosts" not in options:
                log_to_postgres("The hosts parameter is needed, setting to localhost.", WARNING)
self.hosts = options.get("hosts", "localhost").split(",")
    if "port" not in options:
        log_to_postgres("The port parameter is needed, setting to 9042.", WARNING)
            self.port = options.get("port", "9042")
            if (("keyspace" not in options) or ("columnfamily" not in options)) and ("query" not in options):
                log_to_postgres("Either query or columnfamily and keyspace parameter is required.", ERROR)
            self.columnfamily = options.get("columnfamily", None)
            self.keyspace = options.get("keyspace", None)
            self.query = options.get("query", None)
            self.limit = options.get("limit", None)
            self.timeout = options.get("timeout", None)
            self.log_level = options.get("log_level", "0")
    
        super(CassandraFDW, self).__init__(options, columns)
        
        get_options()
        init_connection()
    get_index_info()

def execute(self, quals, columns):
    if self.query:
        statement = self.query
        else:
            statement, used_quals = self.generate_query(columns, quals)
    if self.log_level != "0":
        log_to_postgres(u"CQL query: {0}".format(statement), INFO)
        
        result = self.session.execute(statement)
        for row in result:
            line = {}
            idx = 0
            for column_name in columns:
                value = row[idx]
                if isinstance(value, Decimal):
                    if math.isnan(value) or math.isinf(value):
                        line[column_name] = 0
                    else:
                        line[column_name] = round(value, 10)
                else:
                    line[column_name] = value
                if column_name in used_quals:
                    line[column_name] = used_quals[column_name]
                idx += 1
            yield line

def generate_query(self, columns, quals):
    used_quals = {}
        statement = u"SELECT {0} FROM {1}.{2}".format(",".join(columns), self.keyspace, self.columnfamily)
        # TODO don't query when clustering key is queried and partion key isn't
        is_where = None
        for qual in quals:
            if qual.operator == "=" \
                and qual.field_name in self.queryableColumns \
                    and qual.field_name not in used_quals:
                used_quals[qual.field_name] = qual.value
                if is_where:
                    statement += u" AND {0} = {1} ".format(qual.field_name, format_qual_value(qual))
            else:
                statement += u" WHERE {0} = {1} ".format(qual.field_name, format_qual_value(qual))
                    is_where = 1
    statement += " ALLOW FILTERING "
        if self.limit:
            statement += u" limit {0}".format(self.limit)
return statement, used_quals
    
    def get_path_keys(self):
        s = [(v, k.encode('ascii', 'ignore')) for k, v in self.queryableColumns.iteritems()]
        d = defaultdict(list)
        for k, v in s:
            d[k].append(v)
        return [(tuple(v), k) for k, v in d.items()]
