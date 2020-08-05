CREATE TABLE ods_k (
    id BIGINT,
    name STRING
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'wuren_foo',
    'connector.properties.bootstrap.servers' = 'localhost:9092',
    'connector.properties.zookeeper.connect' = '',
    'connector.properties.group.id' = 'g',
    'format.type' = 'json',
    'update-mode' = 'append'
);

CREATE TABLE ads_m (
    id BIGINT,
    name VARCHAR
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink_dev',
    'connector.table' = 'ads_m',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.write.flush.max-rows' = '1'
);

INSERT INTO ads_m SELECT id, name FROM ods_k;