package org.myorg.quickstart.demo0WordCount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.udf.flatMap.SplitString;

/**
 * @ClassName WordCountTask
 * @Description //单词计数demo
 * @Author ccy
 * @Date 2021/4/13 10:26
 * @Version 1.0
 **/
public class WordCountTask {
    public static void main(String[] args) throws Exception {
        getFile();//从文件读取
        // getSocket(args);//从socket流读取
        return;
    }

    private static void getSocket(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //运行参数中增加 -host 10.0.81.88 -port 33335
        //在 10.0.81.88 上执行 nc -l 33335
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");

        DataStreamSource<String> source = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> out = source.flatMap(new SplitString(" "))
                .keyBy(0)
                .sum(1);
        out.print();

        env.execute();
        // 3> (test,1)
        // 2> (hello,1)
        // 3> (hao,1)
        // 3> (you,1)
        // 3> (test,2)
    }




    private static void getFile() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String file = "src\\main\\resources\\hello.txt";
        DataSource<String> source = env.readTextFile(file);

        DataSet<Tuple2<String, Integer>> resultSet = source.flatMap(new SplitString(" "))
                .groupBy(0)
                .sum(1);

        resultSet.print();
        //(OpenSource,1)
        // (get,1)
        // (great,1)
        // (is,1)
        // (student,1)
        // (I,2)
        // (a,2)
        // (am,1)
        // (chance,1)
        // (want,1)
        // (Apahce,1)
        // (Java,1)
        // (change,1)
        // (flink,1)
        // (me,1)
        // (the,1)
        // (world,2)
        // (It,1)
        // (for,1)
        // (have,1)
        // (hello,4)
        // (to,2)
    }
}
