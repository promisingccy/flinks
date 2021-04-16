package org.myorg.quickstart.other.DTransform.reduce;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.udf.reduce.MaxInGroup;
import org.myorg.quickstart.util.MapUtil;

/**
 * @ClassName MaxTempInItem
 * @Description //统计各个 sensor_id 对应温度值的最大值
 * @Author ccy
 * @Date 2021/4/15 14:51
 * @Version 1.0
 **/
public class MaxTempInItem {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> stream = source.map(MapUtil::map);

        //分组
        stream.keyBy("id")
                .reduce(new MaxInGroup())
                .print();

        env.execute();

        // 1> SensorReading{id='sensor_2', timestamp=1547718207, temperature=36.3}
        // 3> SensorReading{id='sensor_6', timestamp=1547718212, temperature=37.1}
        // 3> SensorReading{id='sensor_6', timestamp=1547718225, temperature=37.1}
        // 4> SensorReading{id='sensor_4', timestamp=1547718209, temperature=32.8}
        // 4> SensorReading{id='sensor_7', timestamp=1547718223, temperature=33.6}
        // 4> SensorReading{id='sensor_7', timestamp=1547718202, temperature=33.6}
        // 3> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
        // 3> SensorReading{id='sensor_6', timestamp=1547718201, temperature=37.1}
        // 2> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
        // 2> SensorReading{id='sensor_10', timestamp=1547718220, temperature=38.1}
        // 4> SensorReading{id='sensor_4', timestamp=1547718215, temperature=34.1}
        // 4> SensorReading{id='sensor_4', timestamp=1547718218, temperature=34.1}
    }
}
