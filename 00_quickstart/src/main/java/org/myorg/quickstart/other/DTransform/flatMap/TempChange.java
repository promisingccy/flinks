package org.myorg.quickstart.other.DTransform.flatMap;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.udf.map.TempChangeWarning;
import org.myorg.quickstart.util.MapUtil;

/**
 * @ClassName TempChange
 * @Description // 实现：检测传感器的温度值，如果连续的两个温度差值超过 10 度，就输出报警。
 * @Author ccy
 * @Date 2021/4/14 13:42
 * @Version 1.0
 **/
public class TempChange {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //在 10.0.81.88 上执行 nc -l 33335
        DataStreamSource<String> source = env.socketTextStream("10.0.81.88", 33335);

        SingleOutputStreamOperator<SensorReading> stream = source.map(MapUtil::map);

        stream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0))
                .print();
        //输入流
        // sensor_7,1547718202,6
        // sensor_7,1547718202,66
        // sensor_7,1547718202,644
        // sensor_7,1547718202,6

        //输出流-输出格式为 id/上一次温度/当前温度
        //(sensor_7,6.0,66.0)
        //(sensor_7,66.0,644.0)
        //(sensor_7,644.0,6.0)
        env.execute();
    }

}
