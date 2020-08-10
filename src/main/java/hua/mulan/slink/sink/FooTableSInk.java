package hua.mulan.slink.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/06
 **/
public class FooTableSInk implements AppendStreamTableSink<Row> {

    private String[] fieldNames;
    private DataType[] fieldTypes;

    public FooTableSInk(String[] fieldNames, DataType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.print();
    }

    @Override
    public DataType getConsumedDataType() {
        return getTableSchema().toRowDataType();
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (this.fieldNames != null || this.fieldTypes != null) {
            throw new IllegalStateException(
                "CsvTableSink has already been configured field names and field types.");
        }
        DataType[] dataTypes = Arrays.stream(fieldTypes)
            .map(TypeConversions::fromLegacyInfoToDataType)
            .toArray(DataType[]::new);
        return new FooTableSInk(fieldNames, dataTypes);
    }
}

