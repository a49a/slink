CREATE TABLE ods_k (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_behavior',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'g',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ads_m (
    id BIGINT,
    name VARCHAR,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/flink_dev',
    'table-name' = 'ads_m',
    'username' = 'root',
    'password' = 'root',
    'sink.buffer-flush.max-rows' = '1'
);

INSERT INTO ads_m SELECT id, name FROM ods_k;