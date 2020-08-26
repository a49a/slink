package hua.mulan.slink.factories.hbase1;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/25
 **/
public class HBaseConfValConsts {
    static final String PRINCIPAL = "hbase/eng-cdh1@DTSTACK.COM";
    static final String G_PRINCIPAL = "hbase/_HOST@DTSTACK.COM";
    static final String TEST_PRINCIPAL = "yijing@DTSTACK.COM";

    static final String KEYTAB_PATH = "/Users/luna/etc/cdh/keytab/hbase-master.keytab";
    static final String WHOLE_KEYTAB_PATH = "/Users/luna/etc/cdh/keytab/hbasew.keytab";

    static final String TEST_KEYTAB_PATH = "/Users/luna/etc/test-cdh/yijing.keytab";
    static final String TEST_KRB5_PATH = "/Users/luna/etc/test-cdh/krb5.conf";

    static final String KRB5_PATH = "/etc/krb5.conf";
    static final String AUTH_TYPE = "kerberos";
    static String TEST_ZK_QUORUM = "cdh2.cdhsite:2181,cdh3.cdhsite:2181,cdh4.cdhsite:2181";

    static String KUDU_QUORUM = "kudu1:2181,kudu2:2181,kudu3:2181";
    //    static String KUDU_QUORUM = "kudu1,kudu2,kudu3";
    static String CDH_QUORUM = "eng-cdh1:2181,eng-cdh2:2181,eng-cdh3:2181";
    static String ZNODE_PARENT = "/hbase";
}
