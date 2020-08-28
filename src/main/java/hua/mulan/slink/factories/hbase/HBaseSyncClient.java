package hua.mulan.slink.factories.hbase;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/25
 **/
public class HBaseSyncClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSyncClient.class);

    private String tableName = "wuren_java";
    private byte[] tableNameBytes = tableName.getBytes();
    Configuration conf;
    private Connection conn = null;
    private Table table = null;
    private ResultScanner resultScanner = null;

    public void init() throws IOException {
        conn = HBaseConnectionFactory.getConnectionWithKerberos();
        table = conn.getTable(TableName.valueOf(tableName));
    }

    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    public void get() throws IOException {
        resultScanner = table.getScanner(new Scan());
        for (Result r : resultScanner) {
            Map<String, Object> kv = new HashedMap();
            for (Cell cell : r.listCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                StringBuilder key = new StringBuilder();
                key.append(family).append(":").append(qualifier).append(":").append(value);
                System.out.println("HBase 结果输出");
                System.out.println(key);
            }
        }
        resultScanner.close();
    }

    public void deleteTable() throws IOException {
        Admin admin = conn.getAdmin();
        //删除指定的表要先disable，然后在删除
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public void createTable() throws IOException {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            //设置表的属性
            HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor family = new HColumnDescriptor("cf1".getBytes());
            td.addFamily(family);
            admin.createTable(td);
        } finally {
            admin.close();
        }
    }

    public void put() throws IOException {
        String rowKey = "1";
        String cf = "cf1";
        String qualifier = "name";
        String val = "foo";
        Put put = new Put(rowKey.getBytes());
        table = conn.getTable(TableName.valueOf(tableName));
        put.addColumn(cf.getBytes(), qualifier.getBytes(), val.getBytes());
        table.put(put);
    }

}
