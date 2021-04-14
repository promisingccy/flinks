package org.myorg.quickstart.other.CProcess;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.udf.process.ContinueUpTemp;
import org.myorg.quickstart.util.MapUtil;

/**
 * @ClassName KeyedProcess
 * @Description //实现：监控温度传感器的温度值，如果温度值在 10 秒钟之内连续上升，则报警。
 * @Author ccy
 * @Date 2021/4/13 16:10
 * @Version 1.0
 **/
public class KeyedProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从socket文件读取
        //在 10.0.81.88 上执行 nc -l 33335
        DataStream<String> source = env.socketTextStream("10.0.81.88", 33335);

        //转换数据类型
        DataStream<SensorReading> stream = source.map(MapUtil::map);

        stream.keyBy("id")
            .process(new ContinueUpTemp(10))// 实现自定义的处理函数
            .print();
        // 传感器 sensor_7 温度值连续 10 秒上升

        //sensor_7,1547718202,6
        // sensor_7,1547718202,61
        // sensor_7,1547718202,62
        // sensor_7,1547718202,63  // 传感器 sensor_7 温度值连续 10 秒上升
        // sensor_7,1547718202,6
        // sensor_7,1547718202,61
        // sensor_7,1547718202,62
        // sensor_7,1547718202,63  // 传感器 sensor_7 温度值连续 10 秒上升
        // sensor_7,1547718202,6
        env.execute();
    }
}
