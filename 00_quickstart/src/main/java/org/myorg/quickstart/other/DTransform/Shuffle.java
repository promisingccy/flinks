package org.myorg.quickstart.other.DTransform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.util.MapUtil;

/**
 * @ClassName Shuffle
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/15 16:40
 * @Version 1.0
 **/
public class Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
        DataStream<SensorReading> dataStream = inputStream.map(MapUtil::map);

        // 1、shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");
        // 2、keyBy
        dataStream.keyBy("id").print("keyBy");
        // 3. global
        dataStream.global().print("global");
        env.execute();
    }

    // shuffle> sensor_6,1547718201,15.4
    // shuffle> sensor_7,1547718202,6.7
    // shuffle> sensor_10,1547718205,38.1
    // shuffle> sensor_2,1547718207,36.3
    // shuffle> sensor_4,1547718209,32.8
    // shuffle> sensor_6,1547718212,37.1
    // shuffle> sensor_4,1547718215,34.1
    // shuffle> sensor_4,1547718218,11.89
    // shuffle> sensor_10,1547718220,32.1
    // shuffle> sensor_7,1547718223,33.6
    // shuffle> sensor_6,1547718225,23.22

    // keyBy> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
    // keyBy> SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
    // keyBy> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
    // keyBy> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
    // global> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
    // keyBy> SensorReading{id='sensor_2', timestamp=1547718207, temperature=36.3}
    // global> SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
    // global> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
    // keyBy> SensorReading{id='sensor_4', timestamp=1547718209, temperature=32.8}
    // global> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
    // keyBy> SensorReading{id='sensor_6', timestamp=1547718212, temperature=37.1}
    // global> SensorReading{id='sensor_2', timestamp=1547718207, temperature=36.3}
    // keyBy> SensorReading{id='sensor_4', timestamp=1547718215, temperature=34.1}
    // global> SensorReading{id='sensor_4', timestamp=1547718209, temperature=32.8}
    // keyBy> SensorReading{id='sensor_4', timestamp=1547718218, temperature=11.89}
    // global> SensorReading{id='sensor_6', timestamp=1547718212, temperature=37.1}
    // global> SensorReading{id='sensor_4', timestamp=1547718215, temperature=34.1}
    // keyBy> SensorReading{id='sensor_10', timestamp=1547718220, temperature=32.1}
    // keyBy> SensorReading{id='sensor_7', timestamp=1547718223, temperature=33.6}
    // global> SensorReading{id='sensor_4', timestamp=1547718218, temperature=11.89}
    // keyBy> SensorReading{id='sensor_6', timestamp=1547718225, temperature=23.22}
    // global> SensorReading{id='sensor_10', timestamp=1547718220, temperature=32.1}
    // global> SensorReading{id='sensor_7', timestamp=1547718223, temperature=33.6}
    // global> SensorReading{id='sensor_6', timestamp=1547718225, temperature=23.22}
}
