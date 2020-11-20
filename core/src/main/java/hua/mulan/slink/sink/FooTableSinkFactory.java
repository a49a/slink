package hua.mulan.slink.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/06
 **/
public class FooTableSinkFactory implements StreamTableSinkFactory<Row> {

    String[] fieldNames;
    DataType[] fieldTypes;

    public FooTableSinkFactory() {
        fieldNames = new String[] {
            "id",
            "name"
        };
        fieldTypes = new DataType[] {
            DataTypes.BIGINT(),
            DataTypes.STRING()
        };
    }

    /**
     * ClassLoader加载所有TableFactory，根据这个方法的返回值觉得使用哪个Factory
     * 如果没有任何匹配的TableFactory，报错TableException: findAndCreateTableSource failed.
     * @return
     */
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC); // jdbc
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    /**
     * 所有属性都与下面函数返回值匹配才可以 TableFactoryService会调用如下方法
     * @return
     */
    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // connector
        properties.add(CONNECTOR_PATH);
        // format

        properties.add(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA);
        properties.add(FORMAT_FIELD_DELIMITER);
        properties.add(CONNECTOR_PATH);

        // schema watermark
        properties.add(SCHEMA + "." + DescriptorProperties.WATERMARK + ".*");
        return properties;
    }

    // 1.10
    @Override
    public TableSink<Row> createTableSink(Map<String, String> properties) {
        return new FooTableSink(fieldNames, fieldTypes);
    }
}
