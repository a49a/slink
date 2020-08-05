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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;

public class RedisLookupableTableSource implements LookupableTableSource<BaseRow> {

    @Override
    public TableFunction<BaseRow> getLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public AsyncTableFunction<BaseRow> getAsyncLookupFunction(String[] strings) {
        return new RedisAsyncTableFunction();
    }

    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    @Override
    public DataType getProducedDataType() {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

}
