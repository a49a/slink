package hua.mulan.slink;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/20
 **/
public class SqlChecker {
    public static void main(String[] args) {
        StreamTableEnvironment tEnv = EnvFactory.createTableEnv();
        tEnv.explainSql("");
    }
}
