# flink-connector-elasticsearch

### Install

`cp flink-connector-elasticsearch-sink-1.0.jar /opt/flink/lib/`

#### ignore write some rowdata columns especially whitch in PRIMARY KEY to elasticsearch

for example

[orginal connectors elasticsearch](https://github.com/apache/flink/tree/release-1.13.1/flink-connectors/flink-connector-elasticsearch7)

```sql
CREATE TABLE myUserTable (
  user_id STRING,
  user_name STRING
  uv BIGINT,
  pv BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'users'
);

insert into myUserTable...
```

will write doc with user_id
```json
{
  "_index": "user_index",
  "_type": "_doc",
  "_id": "user_id_0000001",
  "_score": 1,
  "_source": {
    "user_id": "user_id_0000001",
    "user_name": "cclient"
  }
}    
```

some case need doc without user_id like

```json
{
  "_index": "user_index",
  "_type": "_doc",
  "_id": "user_id_0000001",
  "_score": 1,
  "_source": {
    "user_name": "cclient"
  }
}    
```

so need ignore/filter some columns

User Example 

```java
CREATE TABLE myUserTable (
  index STRING,
  user_id STRING,
  user_name STRING
  uv BIGINT,
  pv BIGINT,
  PRIMARY KEY (user_id,user_name) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7-ignore',
  'hosts' = 'http://localhost:9200',
  'index' = '{index}',
  'ignore-fields'='index,id'
);

```

