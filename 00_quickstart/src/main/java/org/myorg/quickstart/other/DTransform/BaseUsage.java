package org.myorg.quickstart.other.DTransform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.udf.filter.StartWith;
import org.myorg.quickstart.udf.flatMap.SplitString;
import org.myorg.quickstart.udf.map.GetStrLength;

/**
 * @ClassName BaseUsage
 * @Description //转换算子 的基本使用
 * @Author ccy
 * @Date 2021/4/15 10:54
 * @Version 1.0
 **/
public class BaseUsage {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("src/main/resources/sensor.txt");

        //获取长度
        SingleOutputStreamOperator<Integer> mapStream = source.map(new GetStrLength());

        //分隔输出
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = source.flatMap(new SplitString(","));

        //过滤
        SingleOutputStreamOperator<String> filterStream = source.filter(new StartWith("sensor_1"));

        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();

        // map:1> 24
        // map:1> 25
        // map:2> 24
        // map:2> 24
        // map:4> 24
        // map:4> 25
        // map:3> 24
        // map:3> 24
        // map:4> 25
        // map:3> 24
        // map:2> 23
        // map:2> 25
        //
        // (sensor_2,1547718207,36.3,1) ....
        // flatMap:3> (sensor_2,1)
        // flatMap:3> (1547718207,1)
        // flatMap:3> (36.3,1)
        //
        // filter:2> sensor_1,1547718199,35.8
        // filter:4> sensor_10,1547718220,32.1
        // filter:2> sensor_10,1547718205,38.1
    }


}
