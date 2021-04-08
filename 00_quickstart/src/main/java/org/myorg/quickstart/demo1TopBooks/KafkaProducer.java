package org.myorg.quickstart.demo1TopBooks;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.myorg.quickstart.demo2Kafka.KafkaInfo;

import java.util.Properties;

/**
 * @ClassName KafkaProducer
 * @Description //TODO
 * @Author ccy
 * @Date 2021/3/18 16:57
 * @Version 1.0
 **/
public class KafkaProducer {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties properties = KafkaInfo.getKafkaProperties();
//new FlinkKafkaProducer("topn",new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("topicOne",new SimpleStringSchema(),properties);
        /*
        //event-timestamp事件的发生时间
        producer.setWriteTimestampToKafka(true);
        */
        text.addSink(producer);
        env.execute();
    }
}//
