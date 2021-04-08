package org.myorg.quickstart.demo2Kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName KafkaProducer
 * @Description //kafka作为flink sink
 * https://mp.weixin.qq.com/s?__biz=MzU3MzgwNTU2Mg==&mid=2247484154&idx=2&sn=611b0123c759a77c901361763db0b130&chksm=fd3d446fca4acd79ac4935f7aa09fb6895820ca1299978d278a5dc3b07cb9c18b47f62f5e88d&scene=21#wechat_redirect
 * @Author ccy
 * @Date 2021/3/17 9:54
 * @Version 1.0
 **/
public class KafkaProducer {
    /**
     * kafka作为flink sink
     * flink写入kafka
     */
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

}
