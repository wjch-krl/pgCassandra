from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from collections import defaultdict

class IndexType:
	PRIMARY_KEY = 1
	PARTITION_KEY= 1
	CLUSTERING_KEY = 100
	SECONDARY_IDX = 1000
	CUSTOM_IDX = 10000
	
class CassandraFDW(ForeignDataWrapper):
	
	def keyTypeToCost(self,columnKeyType,columnIdxType):
		if (columnKeyType == "clustering_key"):
			return IndexType.CLUSTERING_KEY
		if (columnKeyType == "partition_key"):
			return IndexType.PARTITION_KEY
		if (columnIdxType == "CUSTOM")
			return IndexType.CUSTOM_IDX
		if(columnIdxType == "SECONDARY")	
			return IndexType.SECONDARY_IDX
	
	def qualValueToString(self, qual):
		if ((type(qual.value) is str) or (type(qual.value) is unicode)):
			return "'{0}'".format(qual.value)
		return "{0}".format(qual.value)
	
	def __init__(self, options, columns):
		super(CassandraFDW, self).__init__(options, columns)
		# get options
		if "hosts" not in options:
			log_to_postgres("The hosts parameter is needed, setting to localhost.", WARNING)
		hosts = options.get("hosts", "localhost").split(",");
		if "port" not in options:
			log_to_postgres("The port parameter is needed, setting to 9042.", WARNING)
		self.port = options.get("port", "9042")
		if (("keyspace" not in options) or ("columnfamily" not in options)) and ("query" not in options):
			log_to_postgres("Either query or columnfamily and keyspace parameter is required.", ERROR)
		self.columnfamily = options.get("columnfamily", None)
		self.keyspace = options.get("keyspace", None)
		self.query = options.get("query", None)
		self.limit = options.get("limit", None)
		timeout =  options.get("timeout", None)
		
		# Cassandra connection init
		self.cluster =  Cluster(hosts)
		self.session = self.cluster.connect()
		if (timeout):
			self.session.default_timeout = timeout
		# Get querable columns
		tableInfoQuery = "select column_name,type,index_type from system.schema_columns where keyspace_name = '{0}' and columnfamily_name = '{1}'".format(self.keyspace,self.columnfamily)
		result = self.session.execute(tableInfoQuery)
		self.queryableColumns = {}
		for row in result:
			columnName = row[0]
			columnKeyType = row[1]
			columnIdxType = row[2]
			if (columnKeyType == "regular") and (columnIdxType == None):
				continue
			self.queryableColumns[columnName] = self.keyTypeToCost(columnKeyType,columnIdxType)	
	
	
	def execute(self, quals, columns):
		statement = ""
		usedQuals = {}
		if (self.query):
			statement = self.query
		else:
			statement = "SELECT {0} FROM {1}.{2}".format(",".join(columns), self.keyspace, self.columnfamily);
		# TODO don't query when clustering key is queried and partion key isn't
			isWhere = None
			for qual in quals:
				if (qual.operator == "="):
					if (qual.field_name in self.queryableColumns):
						usedQuals[qual.field_name] = qual.value
						if isWhere:
							statement += " AND {0} = {1} ".format(qual.field_name,self.qualValueToString(qual))
						else:
							statement += " WHERE {0} = {1} ".format(qual.field_name,self.qualValueToString(qual))
							isWhere = 1
		if (self.limit):
			statement += " limit {0}".format(limit);
		#log_to_postgres("CQL query: {0}".format(statement), DEBUG)
		
		result = self.session.execute(statement)
		for row in result:
			line = {}
			idx = 0
			for column_name in columns:
				if (key in usedQuals):
					line[column_name] = d[column_name]
				else:
					line[column_name] = row[idx]
				idx = idx + 1
			yield line
			 
	
	def get_path_keys(self):
		s = [(v,k.encode('ascii','ignore')) for k,v in self.queryableColumns.iteritems()]
		d = defaultdict(list)
		for k, v in s:
			d[k].append(v)
		return [(tuple(v),k) for k,v in d.items()]
