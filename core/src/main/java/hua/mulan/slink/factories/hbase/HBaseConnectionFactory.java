package hua.mulan.slink.factories.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/25
 **/
public class HBaseConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnectionFactory.class);
    private static final String ZNODE = "/hbase";

//    static String KUDU_QUORUM = "kudu1:2181,kudu2:2181,kudu3:2181";
//    static String KUDU_QUORUM = "kudu1,kudu2,kudu3";
    public final static String ZK_QUORUM = "hbase.zookeeper.quorum";
    public final static String NODE_PARENT = "hbase.zookeeper.znode.parent";

    public static Connection getConnectionWithKerberos() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBaseConfKeyConsts.ZK_QUORUM, HBaseConfValConsts.CDH_QUORUM);
        conf.set(HBaseConfKeyConsts.ZNODE_PARENT, ZNODE);
        // Kerberos 配置
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

//        conf.set("hadoop.security.authentication", HBaseConfValConsts.AUTH_TYPE);

//        conf.setBoolean("hadoop.security.authorization", true);
//        conf.setBoolean("hbase.security.authorization", true);

//        conf.set("hbase.rpc.protection", "authentication");
          conf.set("hbase.master.kerberos.principal", HBaseConfValConsts.G_PRINCIPAL);
//        conf.set("keytab.file", KEYTAB_PATH);
//        conf.set("kerberos.principal", HBaseConfValConsts.G_PRINCIPAL);

        conf.set("hbase.security.authentication", HBaseConfValConsts.AUTH_TYPE);
        System.setProperty(HBaseConfKeyConsts.KRB5_CONF, HBaseConfValConsts.KRB5_PATH);
        conf.set("hbase.regionserver.kerberos.principal", HBaseConfValConsts.G_PRINCIPAL);

//        conf.setInt("hbase.client.operation.timeout",24000);
//        conf.setInt("hbase.rpc.timeout",10000);
//        conf.setInt("hbase.client.scanner.timeout.period",10000);

        UserGroupInformation userGroupInformation = HbaseConfigUtils.loginAndReturnUGI(conf, HBaseConfValConsts.PRINCIPAL, HBaseConfValConsts.KEYTAB_PATH);
        // TODO delete
        UserGroupInformation.setLoginUser(userGroupInformation);
        Connection conn = userGroupInformation.doAs((PrivilegedAction<Connection>) () -> {
            try {
                return ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                LOG.error("Get connection fail with config:{}", conf);
                throw new RuntimeException(e);
            }
        });

        return conn;
    }

    public static Connection getConnection() {
        Configuration conf = HBaseConfiguration.create();
//        conf.set(ZK_PORT, "2181");
        conf.setInt("hbase.client.operation.timeout",15000);
//        conf.set(ZK_QUORUM, KUDU_QUORUM);
        conf.set(NODE_PARENT, ZNODE);
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

}
