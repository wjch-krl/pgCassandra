# pgCassandra 
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
### 2. Usage
```SQL
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
Select * from post where id = '123124';
```
### 3. Feateures
--Query pushdown
--Custom indexes support (like https://github.com/Stratio/cassandra-lucene-index.git)
