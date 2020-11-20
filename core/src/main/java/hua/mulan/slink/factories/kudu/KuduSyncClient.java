package hua.mulan.slink.factories.kudu;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/09/02
 **/
public class KuduSyncClient {
    private static final Logger LOG = LoggerFactory.getLogger(KuduSyncClient.class);
    /**
     * 获取连接的尝试次数
     */
    private static final int CONN_RETRY_NUM = 3;
    /**
     * 缓存条数
     */
    private static final Long FETCH_SIZE = 1000L;

    private KuduClient client;

    private KuduTable table;

    public static void main(String[] args) {
        initKerberosEnv(true);
        KuduClient client = getKuduClient();
//        KuduClient client = getClient();

        String tableName = "wuren_foo";
//        createTableData(client, tableName);
//        upsertTableData(client, tableName,1);
        getTableData(client, tableName,"name");
        /*
         * 创建一个表名
         * */
//        KuduApiTest.createTableData(client,"zhckudutest1");
        /*
         *列出kudu下的所有表
         * */
//        KuduApiTest.tableListShow(client);
        /*
         * 向指定的kudu表中upsert数据
         * */
        try {
            client.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public static KuduClient getClient() {
        String masters = "172.16.101.13:7051,172.16.100.132:7051,172.16.100.105:7051";
        String kuduMasters = System.getProperty("kuduMasters", masters);
        KuduClient client = new KuduClient.KuduClientBuilder(kuduMasters).build();
        return client;
    }

    public static KuduClient getKuduClient() {
        KuduClient client = null;
        try {
            client = UserGroupInformation.getLoginUser().doAs(
                new PrivilegedExceptionAction<KuduClient>() {
                    @Override
                    public KuduClient run() throws Exception {
                        String kuduMasters = System.getProperty("kuduMasters", "eng-cdh1:7051");

                        return new KuduClient.KuduClientBuilder(kuduMasters).build();
                    }
                }
            );

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return client;
    }

    public static void initKerberosEnv(Boolean debug) {
        try {
            System.setProperty("java.security.krb5.conf","/Users/luna/etc/cdh/keytab/krb5.conf");
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            if (debug){
                System.setProperty("sun.security.krb5.debug", "true");
            }
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("impala/eng-cdh3@DTSTACK.COM", "/Users/luna/etc/cdh/keytab/impalad-cdh3.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void createTableData(KuduClient client, String tableName) {
        List<ColumnSchema> columns = new ArrayList<>();
        //在添加列时可以指定每一列的压缩格式
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).
            compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).
            compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        Schema schema = new Schema(columns);
        CreateTableOptions createTableOptions = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>();
        hashKeys.add("id");
        int numBuckets = 8;
        createTableOptions.addHashPartitions(hashKeys, numBuckets);

        try {
            if (!client.tableExists(tableName)) {
                client.createTable(tableName, schema, createTableOptions);
            }
            System.out.println("成功创建Kudu表：" + tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取kudu表里面的数据
     */
    public static void getTableData(KuduClient client, String table, String columns) {
        try {
            KuduTable kudutable = client.openTable( table);
            KuduScanner kuduScanner = client.newScannerBuilder(kudutable).build();
            while (kuduScanner.hasMoreRows()) {
                RowResultIterator rowResultIterator = kuduScanner.nextRows();
                while (rowResultIterator.hasNext()) {
                    RowResult rowResult = rowResultIterator.next();
//                    System.out.println(rowResult.getInt(columns));
                    System.out.println(rowResult.getString(columns));
                }
            }
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 向kudu表里面插入数据
     */

    public static void upsertTableData(KuduClient client, String tableName, int numRows) {
        try {
            KuduTable kuduTable = client.openTable(tableName);
            KuduSession kuduSession = client.newSession();
            //设置Kudu提交数据方式，这里设置的为手动刷新，默认为自动提交
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            for(int i =0; i < numRows; i++) {
                String userInfo_str = "abcdef,ghigk";
                Insert upsert = kuduTable.newInsert();
                PartialRow row = upsert.getRow();
                String[] userInfo = userInfo_str.split(",");
                if(userInfo.length == 2) {
                    row.addInt("id", 0);
                    row.addString("name", userInfo[1]);
                }
                kuduSession.apply(upsert);
            }
            kuduSession.flush();
            kuduSession.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

//    public void conn() {
//        KuduScanner scanner = null;
//        try {
//            for (int i = 0; i < CONN_RETRY_NUM; i++) {
//                try {
//                    scanner = getConn(tableInfo);
//                    break;
//                } catch (Exception e) {
//                    if (i == CONN_RETRY_NUM - 1) {
//                        throw new RuntimeException("", e);
//                    }
//                    try {
//                        String connInfo = "kuduMasters:" + tableInfo.getKuduMasters() + ";tableName:" + tableInfo.getTableName();
//                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
//                        Thread.sleep(5 * 1000);
//                    } catch (InterruptedException e1) {
//                        LOG.error("",e1);
//                    }
//                }
//            }
//            //load data from table
//            assert scanner != null;
//            String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields(), ",");
//
//
//            while (scanner.hasMoreRows()) {
//                RowResultIterator results = scanner.nextRows();
//                while (results.hasNext()) {
//                    RowResult result = results.next();
//                    Map<String, Object> oneRow = Maps.newHashMap();
//                    for (String sideFieldName1 : sideFieldNames) {
//                        String sideFieldName = sideFieldName1.trim();
//                        ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
//                        if (null != columnSchema) {
//                            KuduUtil.setMapValue(columnSchema.getType(), oneRow, sideFieldName, result);
//                        }
//                    }
//                    String cacheKey = buildKey(oneRow, sideInfo.getEqualFieldList());
//                    List<Map<String, Object>> list = tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList());
//                    list.add(oneRow);
//                }
//            }
//
//        } catch (Exception e) {
//            LOG.error("", e);
//        } finally {
//            if (null != scanner) {
//                try {
//                    scanner.close();
//                } catch (KuduException e) {
//                    LOG.error("Error while closing scanner.", e);
//                }
//            }
//        }
//    }
//
//    private KuduScanner getConn() {
//        String master = "eng-cdh1:";
//        String tableName = "foo";
//        Integer workerCount = 1;
//        Integer socketReadTimeoutMs = 600000;
//        Integer operationTimeoutMs = 600000;
//
//        try {
//            if (client == null) {
//                String kuduMasters = master;
//                Integer defaultSocketReadTimeoutMs = socketReadTimeoutMs;
//                Integer defaultOperationTimeoutMs = operationTimeoutMs;
//
//                Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");
//
//                KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMasters);
//                if (null != workerCount) {
//                    kuduClientBuilder.workerCount(workerCount);
//                }
//
//                if (null != defaultOperationTimeoutMs) {
//                    kuduClientBuilder.defaultOperationTimeoutMs(defaultOperationTimeoutMs);
//                }
//                client = kuduClientBuilder.build();
//
//                if (!client.tableExists(tableName)) {
//                    throw new IllegalArgumentException("Table Open Failed , please check table exists");
//                }
//                table = client.openTable(tableName);
//            }
//            Schema schema = table.getSchema();
//            KuduScanner.KuduScannerBuilder tokenBuilder = client.newScannerBuilder(table);
//            return buildScanner(tokenBuilder, schema);
//        } catch (Exception e) {
//            LOG.error("connect kudu is error:" + e.getMessage());
//            throw new RuntimeException(e);
//        }
//    }
//
//    private KuduScanner buildScanner(KuduScanner.KuduScannerBuilder builder, Schema schema) {
//        Integer batchSizeBytes = tableInfo.getBatchSizeBytes();
//        Long limitNum = tableInfo.getLimitNum();
//        Boolean isFaultTolerant = tableInfo.getFaultTolerant();
//        //查询需要的字段
//        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields(), ",");
//        //主键过滤条件 主键最小值
//        String lowerBoundPrimaryKey = tableInfo.getLowerBoundPrimaryKey();
//        //主键过滤条件 主键最大值
//        String upperBoundPrimaryKey = tableInfo.getUpperBoundPrimaryKey();
//        //主键字段
//        String primaryKeys = tableInfo.getPrimaryKey();
//        if (null == limitNum || limitNum <= 0) {
//            builder.limit(FETCH_SIZE);
//        } else {
//            builder.limit(limitNum);
//        }
//        if (null != batchSizeBytes) {
//            builder.batchSizeBytes(batchSizeBytes);
//        }
//        if (null != isFaultTolerant) {
//            builder.setFaultTolerant(isFaultTolerant);
//        }
//        //  填充谓词信息
//        List<PredicateInfo> predicateInfoes = sideInfo.getSideTableInfo().getPredicateInfoes();
//        if (predicateInfoes.size() > 0) {
//            predicateInfoes.stream().map(info -> {
//                KuduPredicate kuduPredicate = KuduUtil.buildKuduPredicate(schema, info);
//                if (null != kuduPredicate) {
//                    builder.addPredicate(kuduPredicate);
//                }
//                return info;
//            }).count();
//        }
//
//        if (null != lowerBoundPrimaryKey && null != upperBoundPrimaryKey && null != primaryKeys) {
//            List<ColumnSchema> columnSchemas = schema.getPrimaryKeyColumns();
//            Map<String, Integer> columnName = new HashMap<String, Integer>(columnSchemas.size());
//            for (int i = 0; i < columnSchemas.size(); i++) {
//                columnName.put(columnSchemas.get(i).getName(), i);
//            }
//            String[] primaryKey = splitString(primaryKeys);
//            String[] lowerBounds = splitString(lowerBoundPrimaryKey);
//            String[] upperBounds = splitString(upperBoundPrimaryKey);
//            PartialRow lowerPartialRow = schema.newPartialRow();
//            PartialRow upperPartialRow = schema.newPartialRow();
//            for (int i = 0; i < primaryKey.length; i++) {
//                Integer index = columnName.get(primaryKey[i]);
//                KuduUtil.primaryKeyRange(lowerPartialRow, columnSchemas.get(index).getType(), primaryKey[i], lowerBounds[i]);
//                KuduUtil.primaryKeyRange(upperPartialRow, columnSchemas.get(index).getType(), primaryKey[i], upperBounds[i]);
//            }
//            builder.lowerBound(lowerPartialRow);
//            builder.exclusiveUpperBound(upperPartialRow);
//        }
//        List<String> projectColumns = Arrays.asList(sideFieldNames);
//        return builder.setProjectedColumnNames(projectColumns).build();
//    }
}
