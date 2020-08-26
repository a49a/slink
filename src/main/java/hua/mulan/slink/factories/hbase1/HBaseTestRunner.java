package hua.mulan.slink.factories.hbase1;

import sun.security.krb5.KrbException;

import java.io.IOException;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/25
 **/
public class HBaseTestRunner {
    public static void main(String[] args) throws IOException, KrbException {
//        async();
        sync();
    }

    public static void sync() throws IOException {
        HBaseSyncClient client = new HBaseSyncClient();
        try  {
            client.init();
//            client.createTable();
            client.put();
            client.get();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            client.close();
        }
    }
    public static void async() throws KrbException {
        HBaseAsyncClient client = new HBaseAsyncClient();
        try {
            client.init();
            client.get();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }

}
