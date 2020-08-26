package hua.mulan.slink.factories.hbase1;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/25
 **/
public class HBaseConfValConsts {
    static final String PRINCIPAL = "hbase/eng-cdh1@DTSTACK.COM";
    static final String G_PRINCIPAL = "hbase/_HOST@DTSTACK.COM";

    static final String KEYTAB_PATH = "/Users/luna/etc/cdh/keyteb/hbase-master.keytab";
    static final String WHOLE_KEYTAB_PATH = "/Users/luna/etc/cdh/keyteb/hbasew.keytab";

    static final String KRB5_PATH = "/etc/krb5.conf";
    static final String AUTH_TYPE = "Kerberos";
    static String KUDU_QUORUM = "kudu1:2181,kudu2:2181,kudu3:2181";
    //    static String KUDU_QUORUM = "kudu1,kudu2,kudu3";
    static String CDH_QUORUM = "eng-cdh1:2181,eng-cdh2:2181,eng-cdh3:2181";
    static String ZNODE_PARENT = "/hbase";
}
