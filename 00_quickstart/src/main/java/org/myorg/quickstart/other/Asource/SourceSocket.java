package org.myorg.quickstart.other.Asource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.demo0WordCount.WordCountTask;

/**
 * @ClassName SourceSocket
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/13 16:14
 * @Version 1.0
 **/
public class SourceSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //在 10.0.81.88 上执行 nc -l 33335
        DataStreamSource<String> source = env.socketTextStream("10.0.81.88", 33335);
        SingleOutputStreamOperator<Tuple2<String, Integer>> out = source.flatMap(new WordCountTask.MyFlapMap())
                .keyBy(0)
                .sum(1);
        out.print();

        env.execute();
    }
}
