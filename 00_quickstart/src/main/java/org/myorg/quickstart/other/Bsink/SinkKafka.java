package org.myorg.quickstart.other.Bsink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.myorg.quickstart.demo2Kafka.KafkaInfo;
import org.myorg.quickstart.demo2Kafka.MyNoParalleSource;

import java.util.Properties;

/**
 * @ClassName SinkKafka
 * @Description //flink写入kafka
 * @Author ccy
 * @Date 2021/4/13 15:11
 * @Version 1.0
 **/
public class SinkKafka {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties properties = KafkaInfo.getKafkaProperties();
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("topicOne",new SimpleStringSchema(),properties);
        text.addSink(producer);
        env.execute();
    }
}
