package hua.mulan.slink.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.StreamPlanner;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/05
 **/
public class SqlUtils {
    public SqlNode parse(String stmt, StreamTableEnvironment tEnv) {
        FlinkPlannerImpl flinkPlanner = getPlanner(tEnv);
        SqlNode sqlNode = flinkPlanner.parser().parse(stmt);
        return flinkPlanner.validate(sqlNode);
    }

    private static FlinkPlannerImpl getPlanner(StreamTableEnvironment tEnv) {
        StreamTableEnvironmentImpl tEnvImpl = (StreamTableEnvironmentImpl) tEnv;
        StreamPlanner streamPlanner = (StreamPlanner) tEnvImpl.getPlanner();
        org.apache.flink.table.planner.calcite.FlinkPlannerImpl flinkPlanner = streamPlanner.createFlinkPlanner();
        return flinkPlanner;
    }
}
