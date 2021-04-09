package org.myorg.quickstart.other.windowFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName ProcessWindowDemo
 * @Description //！！！暂时有问题
 * @Author ccy
 * @Date 2021/4/8 16:42
 * @Version 1.0
 **/
public class ProcessWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> input = env.fromElements(
                new SensorReading("a1", 10, 1000L),
                new SensorReading("a2", 11, 1100L),
                new SensorReading("a3", 13, 1300L),
                new SensorReading("a4", 14, 1400L));

        input
                .keyBy(x -> x.key)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new MyWastefulMax())
                .print();

        env.execute();
    }

    public static class MyWastefulMax extends ProcessWindowFunction<
                SensorReading,                  // 输入类型
                Tuple3<String, Long, Integer>,  // 输出类型
                String,                         // 键类型
                TimeWindow> {                   // 窗口类型

        @Override
        public void process(
                String key,
                Context context,
                Iterable<SensorReading> events,
                Collector<Tuple3<String, Long, Integer>> out) {

            int max = 0;
            for (SensorReading event : events) {
                max = Math.max(event.value, max);
            }
            out.collect(Tuple3.of(key, context.window().getEnd(), max));
        }
    }
}
