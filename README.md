# DbSink

DbSink is a [Kafka Sink Connector](http://kafka.apache.org/documentation.html#connect) that provides a sink implementation for streaming changes emitted by [Debezium](https://debezium.io/)



## The Advantages compared to other connectors

1.100% compatible with debezium and able to process data change event, schema change event and transaction event  without requiring any  additional transform like 'ExtractNewRecordState'.

2.Supports both transaction-based and table-based parallel applying.

3.Supports for a variety of database dialect applying. Currently only Oracle to Postgres and MySQL to Postgres are supported, more database dialects are being developed  and will be released  in the near future.

4.Supports executing insert, update, and delete based on operation from the debezium event record without requiring additional configuration like 'insert mode'.

5.Support automatic switching to upsert mode to apply insert events when there are duplicate key errors.

6.Supports most data types and able to use the right way to process the data type from the source event based on the definition of the target column.

For example, the time data type of the MySQL can be inserted into either the time data  type or the interval data type  in the postgres database.

7.Support configuration of target table and column. By default, the connector uses the source table and column name as the target ones.



## Data type 

### MySQL to Postgres



| mysql data type | postgres data type                | description                                                  |
| --------------- | :-------------------------------- | ------------------------------------------------------------ |
| float(n)        | float(n)                          | Float in postgres is a standard type while mysql is not, so this may be errors |
| float(n,p)      | float(n) or decimal(n,p)          | No fully equivalent data type, so may be errors              |
| double(n,p)     | double precision or decimal(n,p)  | No fully equivalent data type, so may be errors              |
| double          | double precision                  | Double in postgres is a standard type while mysql is not, so may be errors |
| decimal(n,p)    | decimal(n,p)                      |                                                              |
| bigint          | bigint                            |                                                              |
| mediumint       | int                               |                                                              |
| int             | int                               |                                                              |
| smallint        | smallint                          |                                                              |
| tinyint         | smallint                          |                                                              |
| timestamp(n)    | timestamp(n) with time zone       | Ensure that MySQL and Postgres time zones are the same, in which case no error. |
| datetime(n)     | timestamp(n) without time zone    |                                                              |
| time(n)         | time(n) or interval day to second | 0-23:59:59.999999: time(n) ---> time(n). otherwise time(n) --> interval day to second |
| year            | smallint or interval year         |                                                              |
| bit(n)          | bit(m) m>=n                       |                                                              |
| bit(1)          | bit(1) or bool                    |                                                              |
| tinyint(1)      | bool                              |                                                              |
| binary(n)       | bytea                             |                                                              |
| blob            | bytea                             |                                                              |
| tinyblob        | bytea                             |                                                              |
| mediumblob      | bytea                             |                                                              |
| longblob        | bytea                             |                                                              |
| char(n)         | char(n)                           |                                                              |
| varchar(n)      | varchar(n)                        |                                                              |
| tinytext        | text                              |                                                              |
| text            | text                              |                                                              |
| mediumtext      | text                              |                                                              |
| json            | json                              |                                                              |



# Usage

Here are some examples to introduce the usage method and its application scenarios

## OLTP business system online migration

A trading system is preparing to migrate from MySQL database to Postgres database. In order to verify whether the Postgres database can meet business needs, it is necessary to establish real-time stream replication between MySQL and Postgres, and switch the read-only business of the system to the Postgres database to verify whether the business  system can run normally. Now follow these steps to implement it.



### step1

You need to manually or use third-party tools to convert table schema in mysql to Postgres. The mapping of data types can refer to the [MySQL to Postgres](MySQL to Postgres)

Example:

Mysql:

```
create database shop;
create table shop.user(id int auto_increment primary key, name varchar(50), create_time timestamp(6))
```

Postgres:

```
create schema shop;
create table shop.user(id serial primary key, name varchar(50), create_time timestamp(6))
```



note: the database in mysql will be converted to database in postgres.

### step2

You need download zookeeper,kafka, jre(>=jre11) to create a running environment for the Debezium connector and DbSink connector. There are many such documents online, so we won't go into detail here.



### step3

Write the configuration json for the debezium connector. Here a template is provided directly.

```
{
    "name": "mysql_debezium_source_connector",
     "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.allowPublicKeyRetrieval": true,
    "database.user": "wangwei",
    "database.server.id": "1000",
    "tasks.max": 1,
    "database.include.list": "shop",
    "provide.transaction.metadata": true,
    "schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
    "database.port": 3307,
    "tombstones.on.delete": false,
    "topic.prefix": "olap_migration",
    "schema.history.internal.kafka.topic": "olap_migration_schema_history",
    "database.hostname": "192.168.142.129",
    "database.password": "19961028",
    "snapshot.mode": "initial",
    "snapshot.max.threads": 10,
    "heartbeat.interval.ms": 10000,
    "transforms":"Reroute", 
    "transforms.Reroute.type":"io.debezium.transforms.ByLogicalTableRouter",
    "transforms.Reroute.topic.regex":"(.*)",
    "transforms.Reroute.topic.replacement":"all_topics"
    }
}
```

Here are some special configuration items that you need to notice.

1.snapshot.mode

initial means to capture both snapshot and incremental data.

2.provide.transaction.metadata

In this case, we need get transaction metadata from the source side  in order to apply data in transaction,  so this configuration item must be set to true

3.transforms

Here we configures the 'ByLogicalTableRouter'  transformer  to convert all the topics of source records to the same topic('all_topics'). By default, the topic of a record produced by debezium is related to the table identifier. In this way, the order of records consumed by the sink connector may be different from the order produced by the source connector, because events in different tables will have different topics, only records within the same topic can be ordered



After configuring json, send a post request to kafka Connect to create a connector



### step4

Write the configuration json for the DbSink connector. Here a template is provided directly.

```

{
    "name": "DbSinkConnector",
    "config": {
        "connector.class": "io.dbsink.connector.sink.DbSinkConnector",
        "jdbc.password": "wangwei123",
        "jdbc.username": "wangwei",
        "tasks.max": 1,
        "topics": "all_topics",
        "jdbc.url": "jdbc:postgresql://localhost:5432/migration",
        "database.dialect.name": "PostgreSqlDialect",
        "jdbc.driver.class": "org.postgresql.Driver",
        "jdbc.retries.max": 5,
        "jdbc.backoff.ms": 6000,
        "applier.parallel.max": 50,
        "applier.transaction.enabled": "true",
        "applier.transaction.buffer.size": 10000,
        "applier.worker.buffer.size": 100,
        "table.naming.strategy": "io.dbsink.connector.sink.naming.DefaultTableNamingStrategy",
        "column.naming.strategy": "io.dbsink.connector.sink.naming.DefaultColumnNamingStrategy"
    }
}
```

Here are some special configuration items that you need to notice.

1.apply.transaction.enabled

It specifies whether to apply in a transactional manner. in this case  it's true



2.apply.transaction.buffer.size

it specifies the maximum number of cached transactions,  the larger this value, the larger the heap memory consumes.



3.applier.worker.buffer.size

it specifies the size of the buffer in applier worker,  which indicates the maximum number of transaction which a applier worker can have.



4.applier.parallel.max

it specifies the number of threads to apply parallel



5.table.naming.strategy

It specifies how to resolve the table name from source records,  four strategies are provided:

1.DefaultTableNamingStrategy

"Table1"--> "Table1"

2.LowCaseTableNamingStrategy

"Table1"--> "table1"

3.UpperCaseTableNamingStrategy.

"table"--> "TABLE1"

According to step1, DefaultTableNamingStrategy is used.



6.column.naming.strategy

It specifies how to resolve the column name from source records,  four strategies are provided: 

1.DefaultColumnNamingStrategy

"Col1" ---> "Col1"

2.LowCaseColumnNamingStrategy 

"COL1" ---> "col1"

3.UpperCaseColumnNamingStrategy.

"col1" ---> "COL1"

According to step1, DefaultColumnNamingStrategy is used.



After configuring json, send a post request to kafka Connect to create a connector

