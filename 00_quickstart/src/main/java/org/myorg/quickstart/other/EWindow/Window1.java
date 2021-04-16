package org.myorg.quickstart.other.EWindow;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.util.MapUtil;
import org.myorg.quickstart.util.SourceUtil;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName Window1
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/15 16:47
 * @Version 1.0
 **/
public class Window1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> source = env.addSource(new SourceUtil(), "sensor");
        source.print();
        // // 开窗测试
        // // 1. 增量聚合函数，用于统计某固定时间段内输入数据的个数，每条数据到来就进行计算，保持一个简单的状态。
        // DataStream<Integer> resultStream1 = source.keyBy("id")
        //         .timeWindow(Time.seconds(15)) // 开启一个 15s 的窗口
        //         .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
        //             @Override
        //             public Integer createAccumulator() {
        //                 return 0;
        //             }
        //
        //             //将给定的值输入到累加器中，并返回经过计算之后得出的结果
        //             @Override
        //             public Integer add(SensorReading value, Integer accumulator) {
        //                 return accumulator + 1;
        //             }
        //
        //             //获取经过累加器增量聚合之后的结果
        //             @Override
        //             public Integer getResult(Integer accumulator) {
        //                 return accumulator;
        //             }
        //
        //             @Override
        //             public Integer merge(Integer a, Integer b) {
        //                 return a + b;
        //             }
        //         });
        //
        //
        // resultStream1.print("resultStream1");
        //
        // // 3、其它可选 API
        // OutputTag<SensorReading> outputTag = new OutputTag<>("late") {
        // };
        //
        // SingleOutputStreamOperator<SensorReading> sumStream = source.keyBy("id")
        //         .timeWindow(Time.seconds(15))
        //         .allowedLateness(Time.minutes(1)) // 允许1分钟内的迟到数据<=比如数据产生时间在窗口范围内，但是要处理的时候已经超过窗口时间了
        //         .sideOutputLateData(outputTag) // 侧输出流，迟到超过1分钟的数据，收集于此
        //         .sum("temperature"); // 侧输出流 对 温度信息 求和
        // // 之后可以再用别的程序，把侧输出流的信息和前面窗口的信息聚合。（可以把侧输出流理解为用来批处理来补救处理超时数据）
        // sumStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
