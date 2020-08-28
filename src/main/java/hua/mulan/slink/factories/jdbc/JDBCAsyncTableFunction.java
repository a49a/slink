package hua.mulan.slink.factories.jdbc;

import hua.mulan.slink.util.ThreadFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/24
 **/
public class JDBCAsyncTableFunction extends AsyncTableFunction<Row> {

    private transient ThreadPoolExecutor executor;
    private transient JDBCClient jdbcClient;

    public final static int POOL_SIZE_LIMIT = 1;
    private final static int MAX_TASK_QUEUE_SIZE = 100000;

    private transient SQLClient sqlClient;
    final static String DIVER = "MYSQL_DRIVER";
    @Override
    public void open(FunctionContext context) throws Exception {
        JsonObject clientConfig = new JsonObject();
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        clientConfig
            .put("url", rdbSideTableInfo.getUrl())
            .put("driver_class", DIVER)
            .put("max_pool_size", rdbSideTableInfo.getAsyncPoolSize())
            .put("user", rdbSideTableInfo.getUserName())
            .put("password", rdbSideTableInfo.getPassword())
            .put("provider_class", DT_PROVIDER_CLASS)
            .put("preferred_test_query", PREFERRED_TEST_QUERY_SQL)
            .put("idle_connection_test_period", DEFAULT_IDLE_CONNECTION_TEST_PEROID)
            .put("test_connection_on_checkin", DEFAULT_TEST_CONNECTION_ON_CHECKIN);

        System.setProperty("vertx.disableFileCPResolving", "true");

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(rdbSideTableInfo.getAsyncPoolSize());
        vo.setFileResolverCachingEnabled(false);
        Vertx vertx = Vertx.vertx(vo);
        jdbcClient = JDBCClient.createNonShared(vertx, clientConfig);
        executor = new ThreadPoolExecutor(
            POOL_SIZE_LIMIT,
            POOL_SIZE_LIMIT,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
            new ThreadFactory("jdbcAsyncExec"),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void eval(CompletableFuture<Collection<Row>> outputFuture, String[] input) {
        executor.execute(
            () -> fetch(input, outputFuture)
        );
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
