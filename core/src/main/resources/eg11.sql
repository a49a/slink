CREATE TABLE ods_k (
    id BIGINT,
    dec_s DECIMAL,
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
    dec_s DECIMAL
) WITH (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://localhost:3306/flink_dev',
    'table-name' = 'dec_test',
    'username' = 'root',
    'password' = 'root'
);

-- CREATE VIEW v AS
--     SELECT id, r.ct as name
--     FROM ods_k
--         LEFT JOIN dim_r
--         FOR SYSTEM_TIME AS OF ods_k.pt AS r
--             ON r.key = ods_k.name;
-- INSERT INTO ads_m SELECT * FROM v;
INSERT INTO ads_m
    SELECT id, dec_s
    FROM ods_k;

-- INSERT INTO ads_m
-- SELECT id, ct AS name
-- FROM ods_k LEFT JOIN LATERAL TABLE(lookup_redis(name));