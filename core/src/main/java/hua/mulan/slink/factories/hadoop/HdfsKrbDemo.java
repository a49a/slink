package hua.mulan.slink.factories.hadoop;

import hua.mulan.slink.factories.hbase.HBaseTestRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.PrivilegedAction;
import java.util.*;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/09/19
 **/
public class HdfsKrbDemo {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsKrbDemo.class);

    private Configuration yarnConf;

    static String HADOOP_CONF_DIR = "/Users/luna/opt/hadoop-krb/etc/hadoop";
    private static String HDFS_PRINCIPAL = "hdfs/eng-cdh1@DTSTACK.COM";

    private String HADOOP_KEYTAB = "/Users/luna/etc/hadoop.keytab";

    private String  KRB5_CONF_PATH = "/Users/luna/etc/cdh/keytab/krb5.conf";

    static String  HBASE_MASTER_PRINCIPAL = "hbase/eng-cdh1@DTSTACK.COM";

    static String  HBASE_MASTER_KEYTAB = "/Users/luna/etc/cdh/keytab/hbase-master.keytab";

    private String tokenLocation;

    public void initConf() {
        tokenLocation = "/Users/luna/Downloads/hadooptest/conf/token_delegation";
        System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH);
        yarnConf = YarnConfLoader.getYarnConf(HADOOP_CONF_DIR);
        UserGroupInformation.setConfiguration(yarnConf);
    }

    public void doHdfs() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

        UserGroupInformation ugi2 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(HDFS_PRINCIPAL, HADOOP_KEYTAB);
        ugi2.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                    UserGroupInformation.setLoginUser(ugi);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    UserGroupInformation ugi2 = UserGroupInformation.getLoginUser();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
        UserGroupInformation ugi3 = UserGroupInformation.getCurrentUser();

//        UserGroupInformation ugi = UserGroupInformation.getLoginUser();

//        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(HDFS_PRINCIPAL, HADOOP_KEYTAB);
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                hdfsOperator();
                return null;
            }
        });
    }

    public void operator() throws IOException, YarnException {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);
        for(ApplicationReport report : reportList) {
            System.out.println(report);
        }
    }

    public void hdfsOperator(){
        String fileName = "/";
        Path path = new Path(fileName);
        FileSystem fs;
        try {
            fs = path.getFileSystem(yarnConf);
            FileStatus[] status = fs.listStatus(path);
            LOG.info(String.valueOf(status));
            LOG.info("HDFS success");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getCause());
        }
    }

    public void loginAndSetLoginOtherUser() throws IOException, KrbException {
//        UserGroupInformation.setConfiguration(yarnConf);
        System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH);


        Config.refresh();
        KerberosName.resetDefaultRealm();
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(HBASE_MASTER_PRINCIPAL, HBASE_MASTER_KEYTAB);
        UserGroupInformation.setLoginUser(ugi);
//        UserGroupInformation.setConfiguration(yarnConf);
//        UserGroupInformation ugi2 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal2, keytab2);
//        UserGroupInformation.setLoginUser(ugi2);

        //UserGroupInformation.loginUserFromKeytab(principal2, keytab2);


//        String ticketCachePath = yarnConf.get("hadoop.security.kerberos.ticket.cache.path");
//        UserGroupInformation ugi2 = UserGroupInformation.getBestUGI(ticketCachePath, "hdfs");
//        UserGroupInformation.setLoginUser(ugi2);
    }

    public boolean shouldAuthenticateOverKrb() throws IOException {
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        UserGroupInformation realUser = currentUser.getRealUser();
        if (loginUser !=  null &&
            // Make sure user logged in using Kerberos either keytab or TGT
            loginUser.hasKerberosCredentials() &&
            // relogin only in case it is the login user (e.g. JT)
            // or superuser (like oozie).
            (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
            return true;
        }
        return false;
    }

    public void setTokensFor() throws IOException {
        Credentials credentials = new Credentials();
        Path path = new Path("/");
        TokenCache.obtainTokensForNamenodes(credentials, new Path[]{path}, yarnConf);

        UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();
        Collection<Token<? extends TokenIdentifier>> usrTok = currUsr.getTokens();

        for (Token<? extends TokenIdentifier> token : usrTok) {
            final Text id = new Text(token.getIdentifier());
            LOG.info("Adding user token " + id + " with " + token);
            credentials.addToken(id, token);
        }

        File file = new File(tokenLocation);
        if(file.exists()){
            file.delete();
        }

        file.createNewFile();

        try (DataOutputBuffer dob = new DataOutputBuffer();
             FileChannel fc = new FileOutputStream(file).getChannel()) {
            credentials.writeTokenStorageToStream(dob);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Wrote tokens. Credentials buffer length: " + dob.getLength());
            }

            ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            fc.write(securityTokens);
        }
    }

    public void addCredentials() throws Throwable {
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        // Use reflection API since the API semantics are not available in Hadoop1 profile. Below APIs are
        // used in the context of reading the stored tokens from UGI.
        // Credentials cred = Credentials.readTokenStorageFile(new File(fileLocation), config.hadoopConf);
        // loginUser.addCredentials(cred);
        try {
            Method readTokenStorageFileMethod = Credentials.class.getMethod("readTokenStorageFile",
                File.class, org.apache.hadoop.conf.Configuration.class);
            Credentials cred =
                (Credentials) readTokenStorageFileMethod.invoke(
                    null,
                    new File(tokenLocation),
                    yarnConf);

            // if UGI uses Kerberos keytabs for login, do not load HDFS delegation token since
            // the UGI would prefer the delegation token instead, which eventually expires
            // and does not fallback to using Kerberos tickets
            Method getAllTokensMethod = Credentials.class.getMethod("getAllTokens");
            Credentials credentials = new Credentials();
            //final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
            final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
            Collection<Token<? extends TokenIdentifier>> usrTok = (Collection<Token<? extends TokenIdentifier>>) getAllTokensMethod.invoke(cred);
            //If UGI use keytab for login, do not load HDFS delegation token.
            for (Token<? extends TokenIdentifier> token : usrTok) {
                if (!token.getKind().equals(hdfsDelegationTokenKind)) {
                    final Text id = new Text(token.getIdentifier());
                    credentials.addToken(id, token);
                }
            }

            Method addCredentialsMethod = UserGroupInformation.class.getMethod("addCredentials",
                Credentials.class);
            addCredentialsMethod.invoke(loginUser, credentials);
        } catch (NoSuchMethodException e) {
            LOG.warn("Could not find method implementations in the shaded jar.", e);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public static void main(String[] args) throws Throwable {
        HdfsKrbDemo test = new HdfsKrbDemo();
        test.initConf();
        test.doHdfs();
        test.loginAndSetLoginOtherUser();
//        HBaseTestRunner.sync();
//        test.addCredentials();
        //test.setTokensFor();
//        test.loginAndSetLoginOtherUser();


//        test.hdfsOperator();

        //判断
        boolean shouldAuthenticateOverKrb = test.shouldAuthenticateOverKrb();
        System.out.println(shouldAuthenticateOverKrb);
    }
}

