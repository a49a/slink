package hua.mulan.slink.factories.hbase;

import com.stumbleupon.async.Deferred;
import hua.mulan.slink.util.ThreadFactory;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/25
 **/
public class HBaseAsyncClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseAsyncClient.class);

    private static final int DEFAULT_POOL_SIZE = 16;

    HBaseClient client;

    public void init() throws KrbException, IOException {

        System.setProperty(HBaseConfKeyConsts.KRB5_CONF, HBaseConfValConsts.KRB5_PATH);

        Config config = new Config();
        config.overrideConfig(HBaseConfKeyConsts.ZK_QUORUM, HBaseConfValConsts.CDH_QUORUM);
        config.overrideConfig(HBaseConfKeyConsts.ZNODE_PARENT, HBaseConfValConsts.ZNODE_PARENT);

        config.overrideConfig("hadoop.security.authentication", HBaseConfValConsts.AUTH_TYPE);
        config.overrideConfig("hbase.security.authorization", HBaseConfValConsts.AUTH_TYPE);
        config.overrideConfig("hbase.security.authentication", HBaseConfValConsts.AUTH_TYPE);

        config.overrideConfig("hbase.security.auth.enable", "true");
        config.overrideConfig("hbase.workers.size", "2");

        config.overrideConfig("hbase.sasl.clientconfig", "Client");
        config.overrideConfig("hbase.kerberos.regionserver.principal", HBaseConfValConsts.G_PRINCIPAL);
        System.setProperty("zookeeper.sasl.client", "false");

//        config.overrideConfig("hbase.master.kerberos.principal", HBaseConfValConsts.PRINCIPAL);
        config.overrideConfig("keytab.file", HBaseConfValConsts.KEYTAB_PATH);
//        config.overrideConfig("kerberos.principal", HBaseConfValConsts.PRINCIPAL);

        String jaasFilePath = "/Users/luna/etc/cdh/JAAS.conf";
        System.setProperty(HBaseConfKeyConsts.JAVA_AUTH_LOGIN_CONF, jaasFilePath);
        config.overrideConfig(HBaseConfKeyConsts.JAVA_AUTH_LOGIN_CONF, jaasFilePath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

//        config.overrideConfig("hbase.client.operation.timeout","24000");
//        config.overrideConfig("hbase.rpc.timeout","10000");
//        config.overrideConfig("hbase.client.scanner.timeout.period","10000");

        //refresh
        sun.security.krb5.Config.refresh();
        KerberosName.resetDefaultRealm();
        //reload java.security.auth.login.config
//        javax.security.auth.login.Configuration.setConfiguration(null);

        ExecutorService executorService = new ThreadPoolExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactory("hbase-async"));

        client = new HBaseClient(config, executorService);
//        KerberosClientAuthProvider authProvider = new KerberosClientAuthProvider(client);

    }

    public void initNormal() {
        ExecutorService executorService = new ThreadPoolExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactory("hbase-async"));

        Config config = new Config();
        config.overrideConfig(HBaseConfKeyConsts.ZK_QUORUM, HBaseConfValConsts.KUDU_QUORUM);
        config.overrideConfig(HBaseConfKeyConsts.ZNODE_PARENT, HBaseConfValConsts.ZNODE_PARENT);

        config.overrideConfig("hbase.client.operation.timeout","24000");
        config.overrideConfig("hbase.rpc.timeout","10000");
        config.overrideConfig("hbase.client.scanner.timeout.period","10000");

        client = new HBaseClient(config, executorService);
    }

    public void get() {
        String tableName = "wuren_java";
        try {
            Deferred deferred = client.ensureTableExists(tableName)
                .addCallbacks(arg -> new CheckResult(true, ""), arg -> new CheckResult(false, arg.toString()));

            CheckResult result = (CheckResult) deferred.join();
            if(!result.isConnect()){
                throw new RuntimeException(result.getExceptionMsg());
            }

        } catch (Exception e) {
            throw new RuntimeException("create hbase connection fail:", e);
        }

        GetRequest getRequest = new GetRequest(tableName, "1");
        client.get(getRequest).addCallbacks(arg -> {

            try{
                for (KeyValue keyValue : arg) {
                    System.out.println(keyValue);
                }
            }catch (Exception e){
                LOG.error("get side record exception:", e);
            }

            return "";
        }, arg2 -> {
            LOG.error("get side record exception:" + arg2);
            return "";
        });
    }

    public void close() {
        if (client != null) {
            client.shutdown();
        }
    }

    class CheckResult{

        private boolean connect;

        private String exceptionMsg;

        CheckResult(boolean connect, String msg){
            this.connect = connect;
            this.exceptionMsg = msg;
        }

        public boolean isConnect() {
            return connect;
        }

        public void setConnect(boolean connect) {
            this.connect = connect;
        }

        public String getExceptionMsg() {
            return exceptionMsg;
        }

        public void setExceptionMsg(String exceptionMsg) {
            this.exceptionMsg = exceptionMsg;
        }
    }

}
