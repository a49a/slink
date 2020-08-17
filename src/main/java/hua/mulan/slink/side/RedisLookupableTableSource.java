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

package hua.mulan.slink.side;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class RedisLookupableTableSource implements LookupableTableSource<Row> {

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    public RedisLookupableTableSource() {
        fieldNames = new String[] {
            "key",
            "ct"
        };

        fieldTypes = new TypeInformation[2];
        fieldTypes[0] = Types.STRING;
        fieldTypes[1] = Types.STRING;
    }


    @Override
    public TableFunction<Row> getLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] strings) {
        return new RedisAsyncTableFunction();
    }

    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

//    @Override
//    public DataType getProducedDataType() {
//        return null;
//    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public TableSchema getTableSchema() {

        return TableSchema.builder()
            .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
            .build();
    }

}
