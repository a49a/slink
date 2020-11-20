package hua.mulan.slink.connector.es6;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/11
 **/
public class Es6UpsertTableSink implements UpsertStreamTableSink<Row> {

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        final ElasticsearchSink.Builder<Tuple2<Boolean, Row>> builder =
            new ElasticsearchSink.Builder<>();

        ElasticsearchSink sink = builder.build();
        return dataStream.addSink(sink);
    }

}
