package hua.mulan.slink.connector.es6;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/11
 **/
public class Es6SInk<T> extends ElasticsearchSinkBase<T, RestHighLevelClient> {

    protected EsInsertSinkFunction sinkFunction;

    protected transient Meter outRecordsRate;

    protected Map userConfig;

    public void MetricElasticsearch6Sink(Map userConfig, List transportAddresses,
                                    ElasticsearchSinkFunction elasticsearchSinkFunction,
                                    ElasticsearchTableInfo es6TableInfo) {
        super(new ExtendEs6ApiCallBridge(transportAddresses, es6TableInfo), userConfig, elasticsearchSinkFunction, new NoOpFailureHandler());
        this.customerSinkFunc = (EsInsertSinkFunction) elasticsearchSinkFunction;
        this.userConfig = userConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initMetric();
    }


    public void initMetric() {
        Counter counter = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        Counter outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);

        sinkFunction.setOutRecords(counter);
        sinkFunction.setOutDirtyRecords(outDirtyRecords);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(counter, 20));
    }
}
