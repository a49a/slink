package hua.mulan.slink.connector.jdbc;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/13
 **/
public class VertxSample {
    public static void main(String[] args) {
        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(1);
        vo.setWorkerPoolSize(1);
        Vertx vertx = Vertx.vertx(vo);
        JsonObject config = new JsonObject();
        config.put("url", "jdbc:mysql://localhost:3306/wuren")
            .put("driver_class", "com.mysql.cj.jdbc.Driver")
            .put("max_pool_size", 2)
            .put("user", "root")
            .put("password", "root");

        System.setProperty("vertx.disableFileCPResolving", "true");
        JDBCClient client = JDBCClient.create(vertx, config);
        client.getConnection(res -> {
            if (res.succeeded()) {
                SQLConnection connection = res.result();
                connection.query("SELECT * FROM dec_test", res2 -> {
                    if (res2.succeeded()) {
                        ResultSet rs = res2.result();
                        for (JsonArray line : rs.getResults()) {
                            System.out.println(line);
                        }
                    }
                });
            }
        });

    }
}
