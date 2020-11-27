/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hua.mulan.slink.cli;

import avro.shaded.com.google.common.collect.Maps;
import hua.mulan.slink.EnvFactory;
import hua.mulan.slink.cli.SqlCommandParser.SqlCommandCall;
import hua.mulan.slink.sink.FooTableSinkFactory;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.factories.StreamTableSinkFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String workSpace;
    private TableEnvironment tEnv;

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
    }

    private void run() throws Exception {
        this.tEnv = EnvFactory.createTableEnv();
        StreamTableSinkFactory factory = new FooTableSinkFactory();
        Map<String, String> map = Maps.newHashMap();
//        TableSink sink = factory.createTableSink(map);
//        tEnv.registerTableSink("ads", sink);
//        tEnv.registerTableSource("dim_r", new RedisLookupableTableSource());

        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandCall call : calls) {
            callCommand(call);
        }

//        TableResult tableResult2 = tEnv.sqlQuery("SELECT id, name FROM ods_k").execute();
//        tableResult2.print();
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            TableResult tr = tEnv.executeSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
