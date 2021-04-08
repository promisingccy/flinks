package org.myorg.quickstart.demo2Kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName KafkaConsumer
 * @Description //kafka作为flink source
 * https://mp.weixin.qq.com/s?__biz=MzU3MzgwNTU2Mg==&mid=2247484154&idx=2&sn=611b0123c759a77c901361763db0b130&chksm=fd3d446fca4acd79ac4935f7aa09fb6895820ca1299978d278a5dc3b07cb9c18b47f62f5e88d&scene=21#wechat_redirect
 * @Author ccy
 * @Date 2021/3/17 10:10
 * @Version 1.0
 **/
public class KafkaConsumer {

    /**
     * kafka作为flink source
     * flink读取kafka
     */
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
