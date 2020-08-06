package hua.mulan.slink.sink;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELDS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_NUM_FILES;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_WRITE_MODE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/06
 **/
public class FooTableSinkFactory implements StreamTableSinkFactory<Row> {
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
        context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE);
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        context.put(FORMAT_PROPERTY_VERSION, "1");
        context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // connector
        properties.add(CONNECTOR_PATH);
        // format
        properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.TYPE);
        properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.DATA_TYPE);
        properties.add(FORMAT_FIELDS + ".#." + DescriptorProperties.NAME);
        properties.add(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA);
        properties.add(FORMAT_FIELD_DELIMITER);
        properties.add(CONNECTOR_PATH);
        properties.add(FORMAT_WRITE_MODE);
        properties.add(FORMAT_NUM_FILES);

        // schema
        properties.add(SCHEMA + ".#." + DescriptorProperties.TYPE);
        properties.add(SCHEMA + ".#." + DescriptorProperties.DATA_TYPE);
        properties.add(SCHEMA + ".#." + DescriptorProperties.NAME);
        properties.add(SCHEMA + ".#." + DescriptorProperties.EXPR);
        // schema watermark
        properties.add(SCHEMA + "." + DescriptorProperties.WATERMARK + ".*");
        return properties;
    }

    @Override
    public TableSink<Row> createTableSink(Map<String, String> properties) {

        return null;
    }
}
