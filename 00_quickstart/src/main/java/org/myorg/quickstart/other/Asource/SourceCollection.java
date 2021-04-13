package org.myorg.quickstart.other.Asource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.dto.SensorReading;

import java.util.Arrays;

/**
 * @ClassName SourceCollection
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/13 13:41
 * @Version 1.0
 **/
public class SourceCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置并行度

        // 从集合中读取数据
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));
        source.print();


        // 从元素中读取数据
        DataStream<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
        source1.print();

        env.execute();
    }
    //1
    // 2
    // 3
    // SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
    // 4
    // 5
    // SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
    // 6
    // 7
    // SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
    // 8
    // 9
    // 0
    // SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
}
