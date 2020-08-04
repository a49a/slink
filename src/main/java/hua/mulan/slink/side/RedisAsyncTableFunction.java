package hua.mulan.slink.side;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @program: slink
 * @author: mulan
 * @create: 2020/08/04
 **/
public class RedisAsyncTableFunction extends AsyncTableFunction<BaseRow> {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private RedisKeyAsyncCommands<String, String> async;
    private static final String PREFIX = "redis://";
    private static final String DEFAULT_DB = "0";
    private static final String DEFAULT_URL = "localhost:6379";
    private static final String DEFAULT_PASSWORD = "";

    @Override
    public void open(FunctionContext context) throws Exception {
        final String url = DEFAULT_URL;
        final String password = DEFAULT_PASSWORD;
        final String database = DEFAULT_DB;
        StringBuilder redisUri = new StringBuilder();
        redisUri.append(PREFIX).append(password).append(url).append("/").append(database);

        redisClient = RedisClient.create(redisUri.toString());
        connection = redisClient.connect();
        async = connection.async();
    }

    public void eval(CompletableFuture<BaseRow> outputFuture, String key) {
        RedisFuture<Map<String, String>> redisFuture = ((RedisHashAsyncCommands) async).hgetall(key);
        redisFuture.thenAccept(new Consumer<Map<String, String>>() {
            @Override
            public void accept(Map<String, String> values) {
                int len = 1;
                GenericRow row = new GenericRow(len);
                outputFuture.complete(row);
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (connection != null){
            connection.close();
        }
        if (redisClient != null){
            redisClient.shutdown();
        }
    }
}
