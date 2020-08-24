package hua.mulan.slink.side;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/24
 **/
public class JDBCAsyncTableFunction extends AsyncTableFunction<Row> {

    private transient ThreadPoolExecutor executor;

    public final static int MAX_DB_CONN_POOL_SIZE_LIMIT = 1;
    private final static int MAX_TASK_QUEUE_SIZE = 100000;

    private transient SQLClient sqlClient;


    @Override
    public void open(FunctionContext context) throws Exception {
        executor = new ThreadPoolExecutor(
            MAX_DB_CONN_POOL_SIZE_LIMIT,
            MAX_DB_CONN_POOL_SIZE_LIMIT,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
            new ThreadFactory("jdbcAsyncExec"),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void eval(CompletableFuture<Collection<Row>> outputFuture, String[] input) {
        executor.execute(() -> fetch(input, outputFuture));
    }

    private void fetch(String[] input, CompletableFuture<Collection<Row>> outputFuture) {
        outputFuture.complete(Collections.singletonList(null));
    }

    @Override
    public void close() throws Exception {
        if (sqlClient != null) {
            sqlClient.close();
        }

        if(executor != null){
            executor.shutdown();
        }
    }
}
