package org.myorg.quickstart.demo2Kafka;

import java.util.Properties;

/**
 * @ClassName KafkaInfo
 * @Description //TODO
 * @Author ccy
 * @Date 2021/3/17 10:50
 * @Version 1.0
 **/
public class KafkaInfo {

    public static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.type", "JKS");
        properties.put("ssl.keystore.password", "aa");
        properties.put("ssl.truststore.location", "D:/flink/quickstart/src/main/resources/certs/demo2Kafka/demo2Kafka.client.truststore.jks");
        properties.put("bootstrap.servers", "10.0.81.121:9091");
        properties.put("ssl.truststore.password", "aa");
        properties.put("message.max.bytes", "4194304");
        properties.put("zookeeper.connect", "10.0.81.96:2181");
        properties.put("ssl.endpoint.identification.algorithm", "");
        properties.put("ssl.keystore.location", "D:/flink/quickstart/src/main/resources/certs/demo2Kafka/demo2Kafka.client.keystore.jks");
        properties.put("ssl.key.password", "aa");
        properties.put("group.id", "flinkDemoxxxxyyy");
        properties.put("max.request.size", 10485760);
        properties.put("batch.size", 10 * 1024 * 1024);
        return properties;
    }
}
