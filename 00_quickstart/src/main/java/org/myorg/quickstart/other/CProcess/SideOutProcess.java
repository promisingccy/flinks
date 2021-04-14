package org.myorg.quickstart.other.CProcess;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.util.MapUtil;
import org.myorg.quickstart.udf.process.SideOutByTemp;

/**
 * @ClassName SideOutProcess
 * @Description //实现：监控传感器温度值，将温度值低于 30 度的数据输出到 sideOutput 。
 * @Author ccy
 * @Date 2021/4/13 19:14
 * @Version 1.0
 **/
public class SideOutProcess {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从socket文件读取
        //在 10.0.81.88 上执行 nc -l 33335
        DataStream<String> source = env.socketTextStream("10.0.81.88", 33335);

        //转换数据类型
        DataStream<SensorReading> stream = source.map(MapUtil::map);

        //定义侧输出流 用来输出低温<30
        /**
         * 匿名内部类: new OutputTag<>("lowTemp"){}
         * ！！！！ 这里需要调整运行环境的JDK为11
         * 其实这个是有用的，OutputTag有两个构造函数，
         * 当前用的这个构造函数只有一个参数，传入一个string，用来标识分流的数据是什么含义；
         * 另一个构造函数还可以传入一个TypeInformation对象，这个是用来说明分流的数据是什么类型的。
         *
         * 使用单参数的构造函数，flink会自动推测TypeInformation，
         * 但是OutputTag带了泛型，在编译的时候会丢失这个信息，
         * 所以只能生成匿名内部类把泛型里的信息带上
         */
        OutputTag<SensorReading> lowTemp = new OutputTag<>("lowTemp"){};

        //###分流处理
        SingleOutputStreamOperator<SensorReading> highTemp = stream.process(new SideOutByTemp(lowTemp));

        //输出
        highTemp.print("high-temp");
        highTemp.getSideOutput(lowTemp).print("low-temp");

        env.execute();
        //low-temp> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
        // high-temp> SensorReading{id='sensor_2', timestamp=1547718207, temperature=36.3}
    }
}
