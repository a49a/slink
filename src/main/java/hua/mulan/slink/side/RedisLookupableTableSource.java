package hua.mulan.slink.side;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
        return null;
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
