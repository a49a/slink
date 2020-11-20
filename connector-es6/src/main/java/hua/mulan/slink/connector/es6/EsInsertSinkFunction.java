package hua.mulan.slink.connector.es6;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.types.Row;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/11
 **/
public class EsInsertSinkFunction implements ElasticsearchSinkFunction<Tuple2<Boolean, Row>> {

    private final Logger logger = LoggerFactory.getLogger(EsInsertSinkFunction.class);
    /**
     * 用作ID的属性值连接符号
     */
    private static final String ID_VALUE_SPLIT = "_";

    private String index;

    private String type;

    private List<Integer> idFieldIndexList;

    private List<String> fieldNames;

    private List<String> fieldTypes;

    private TypeInformation<Row> typeInfo;

    private transient Counter outRecords;

    private transient Counter outDirtyRecords;

    public EsInsertSinkFunction(String index, String type, List<String> fieldNames, List<String> fieldTypes, List<Integer> idFieldIndexes) {
        this.index = index;
        this.type = type;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.idFieldIndexList = idFieldIndexes;
    }

    @Override
    public void process(Tuple2 tuple2, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            Tuple2<Boolean, Row> tupleTrans = tuple2;
            Boolean retract = tupleTrans.getField(0);
            Row element = tupleTrans.getField(1);
            if (!retract) {
                return;
            }

            indexer.add(createIndexRequest(element));
            outRecords.inc();
        } catch (Throwable e) {
            outDirtyRecords.inc();
            logger.error("Failed to store source data {}. ", tuple2.getField(1));
            logger.error("Failed to create index request exception. ", e);
        }
    }

    public void setOutRecords(Counter outRecords) {
        this.outRecords = outRecords;
    }

    public void setOutDirtyRecords(Counter outDirtyRecords) {
        this.outDirtyRecords = outDirtyRecords;
    }

    private IndexRequest createIndexRequest(Row element) {
        String idFieldStr = "";
        if (null != idFieldIndexList) {
            // index start at 1,
            idFieldStr = idFieldIndexList.stream()
                .filter(index -> index > 0 && index <= element.getArity())
                .map(index -> element.getField(index - 1).toString())
                .collect(Collectors.joining(ID_VALUE_SPLIT));
        }

        JsonRowSerializationSchema seSchema = JsonRowSerializationSchema
            .builder()
            .withTypeInfo(typeInfo)
            .build();
        byte[] jsonString = seSchema.serialize(element);

        if (StringUtils.isEmpty(idFieldStr)) {
            return Requests.indexRequest()
                .index(index)
                .type(type)
                .source(jsonString, XContentType.JSON);
        }

        return Requests.indexRequest()
            .index(index)
            .type(type)
            .id(idFieldStr)
            .source(jsonString, XContentType.JSON);
    }
}