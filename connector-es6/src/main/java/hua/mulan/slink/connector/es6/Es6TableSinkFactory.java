package hua.mulan.slink.connector.es6;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ElasticsearchValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_ADDRESS;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_AUTHMESH;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_CLUSTER;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_DOCUMENT_TYPE;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_ID;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_INDEX;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_PARALLELISM;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_PASSWORD;
import static hua.mulan.slink.connector.es6.EsValidator.CONNECTOR_USERNAME;
import static hua.mulan.slink.connector.es6.EsValidator.KEY_TRUE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/11
 **/
public class Es6TableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, "es");
        context.put(CONNECTOR_VERSION, "6");
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(CONNECTOR_ADDRESS);
        properties.add(CONNECTOR_CLUSTER);
        properties.add(CONNECTOR_INDEX);
        properties.add(CONNECTOR_DOCUMENT_TYPE);
        properties.add(CONNECTOR_ID);
        properties.add(CONNECTOR_AUTHMESH);
        properties.add(CONNECTOR_USERNAME);
        properties.add(CONNECTOR_PASSWORD);
        properties.add(KEY_TRUE);
        properties.add(CONNECTOR_PARALLELISM);
        return properties;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> createTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        return new Es6UpsertTableSink();
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new StreamTableDescriptorValidator(true, false, true).validate(descriptorProperties);
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        new ElasticsearchValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

}
