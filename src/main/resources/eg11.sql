CREATE TABLE ods_k (
    id BIGINT,
    name VARCHAR
) WITH (
    'connector' = 'kafka',
    'topic' = 'wuren_foo',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'gg',
    'scan.startup.mode' = 'earliest-offset',
--     'format' = 'json'
    'format' = 'json',
    'json.fail-on-missing-field' = 'true',
    'json.ignore-parse-errors' = 'false'
);

CREATE TABLE ads_m (
    id BIGINT,
    name VARCHAR
) WITH (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://localhost:3306/flink_dev',
    'table-name' = 'ads_m',
    'username' = 'root',
    'password' = 'root'
);

-- INSERT INTO ads_m SELECT id, tr(name) FROM ods_k;

INSERT INTO ads_m
    SELECT id, ct AS name
    FROM ods_k, LATERAL TABLE(lookup_redis(name));
