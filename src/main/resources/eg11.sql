CREATE TABLE ods_k (
    id BIGINT,
    name VARCHAR,
    pt AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'wuren_foo',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'gg',
--     'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
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

INSERT INTO ads_m
    SELECT id, r.ct as name
    FROM ods_k
        LEFT JOIN dim_r
        FOR SYSTEM_TIME AS OF ods_k.pt AS r
            ON r.key = ods_k.name;

-- INSERT INTO ads_m
--     SELECT id, name
--     FROM ods_k;

-- INSERT INTO ads_m
-- SELECT id, ct AS name
-- FROM ods_k LEFT JOIN LATERAL TABLE(lookup_redis(name));