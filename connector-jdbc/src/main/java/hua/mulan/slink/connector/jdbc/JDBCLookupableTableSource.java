package hua.mulan.slink.connector.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/26
 **/
public class JDBCLookupableTableSource implements LookupableTableSource<Row> {

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    public void RedisLookupableTableSource() {
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
        return new JDBCAsyncTableFunction();
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
