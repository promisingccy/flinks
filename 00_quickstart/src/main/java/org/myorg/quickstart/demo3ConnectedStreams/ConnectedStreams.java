package org.myorg.quickstart.demo3ConnectedStreams;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName ConnectedStreams
 * @Description //实现流的关联 RichCoFlatMapFunction
 * @Author ccy
 * @Date 2021/4/8 15:41
 * @Version 1.0
 **/
public class ConnectedStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //两个流只有键一致的时候才能连接
        //keyBy 的作用是将流数据分区，key不存在，blocked=true
        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        DataStream<String> streamOfWords = env.fromElements("Apache", "DROP", "Flink", "IGNORE").keyBy(x -> x);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();
        // 输出的内容只能是 streamOfWords 中存在且在 control 中不存在的元素
        // 4> Flink
        // 3> Apache

        env.execute();
    }

    /**
     * RichCoFlatMapFunction 是一种可以被用于一对连接流的 FlatMapFunction
     * 布尔变量 blocked 被用于记录在数据流 control 中出现过的键（在这个例子中是单词），
     * 并且这些单词从 streamOfWords 过滤掉。这是 keyed state，并且它是被两个流共享的。
     *
     * control 流中的元素会进入 flatMap1，
     * streamOfWords 中的元素会进入 flatMap2。
     * 这是由两个流连接的顺序决定的，本例中为 control.connect(streamOfWords)。
     */
    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);//control 流中的元素会进入 flatMap1
        }

        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);//streamOfWords 中的元素会进入 flatMap2
            }
        }
    }
}
