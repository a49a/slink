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

package hua.mulan.slink;

import hua.mulan.slink.factories.redis.RedisAsyncTableFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @program: slink
 * @author: mulan
 * @create: 2020/08/04
 **/
public class EnvFactory {

    public static StreamExecutionEnvironment createEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return env;
    }

    public static StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings =
            EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        registerUdf(bsTableEnv);
        return bsTableEnv;
    }

    private static void registerUdf(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("tr", Tr.class);
        tEnv.createTemporarySystemFunction("lookup_redis", RedisAsyncTableFunction.class);
        tEnv.createTemporarySystemFunction("tf", Tf.class);
//        tEnv.registerFunction("tr", new Tr());
    }

}
