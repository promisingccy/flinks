package org.myorg.quickstart.other.Asource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.myorg.quickstart.demo2Kafka.KafkaInfo;

import java.util.Properties;
/**
 * @ClassName SourceKafka
 * @Description //flink读取kafka
 * @Author ccy
 * @Date 2021/4/13 15:10
 * @Version 1.0
 **/
public class SourceKafka {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = KafkaInfo.getKafkaProperties();

        //topicB-1 partition
        //topicOne-5 partitions
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topicOne", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();
        DataStream<String> stream = env
                .addSource(consumer);
        stream.print();
        //stream.map();
        env.execute();

    }
}
