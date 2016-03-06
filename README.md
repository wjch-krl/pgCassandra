# pgCassandra - devel branch - unstable, some code might be untested
PostgreSQL's multicorn based FDW (Foreign Data Wrapper) for connecting to Apache Cassandra NoSQL database 
### 1. Installation
##### a) Install Multicorn
http://multicorn.readthedocs.org/en/latest/installation.html
##### b) Install python cassandra driver 
```bash
pip install cassandra-driver
```
##### c) Download pgCassandra:
```bash
git clone https://github.com/wjch-krl/pgCassandra
cd pgCassandra
```
##### d) Install extension
```bash
python setup.py install
```
### 2. Example

##### Cassandra


##### Postgresql

```sql
--Create extension and server
CREATE EXTENSION multicorn;
CREATE SERVER multicorn_srv FOREIGN DATA WRAPPER multicorn
options (
  wrapper 'pgCassandra.CassandraFDW',
   port '9042',
   columnfamily 'post',
   keyspace 'social_postsdata',
   hosts '10.233.41.11,10.233.41.12,10.233.41.13'
);

--Create foreign table
CREATE FOREIGN TABLE messages (
    id text,
    luence text,
    message text,
    datecreate timestamp
) 
SERVER multicorn_srv;

--Query created table
Select * from messages where id = '123124';
```

### 3. Feateures
* Automatic index dicovery - query is pushed to cassandra only if it can be performed
* Custom indexes support (e.g https://github.com/Stratio/cassandra-lucene-index.git)


### 4. Aviable options
* hosts - The hosts parameter is needed, setting to localhost.
* port - The port parameter is needed, setting to 9042.
* columnfamily - Either query or columnfamily and keyspace parameter is required.
* keyspace - 
* query - If set query is not generated - insted user provided value is used
* limit - Since multicorn doesnt pushdown limit clause, this can be set to reduce number of rows returned by query
* timeout - Cassandra query timeout in seconds
* log_level - 
