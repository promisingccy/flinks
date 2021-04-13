package org.myorg.quickstart.other.Asource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName SourceFile
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/13 14:17
 * @Version 1.0
 **/
public class SourceFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取文件路径
        String inputPath = "src/main/resources/sensor.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        dataStream.print();
        env.execute();
    }

    //1> sensor_1,1547718199,35.8
    // 1> sensor_6,1547718201,15.4
    // 1> sensor_7,1547718202,6.7
    // 3> sensor_4,1547718215,34.1
    // 3> sensor_4,1547718218,11.89
    // 4> sensor_7,1547718223,33.6
    // 4> sensor_6,1547718225,23.22
    // 1> sensor_10,1547718205,38.1
    // 3> sensor_10,1547718220,32.1
    // 2> sensor_2,1547718207,36.3
    // 2> sensor_4,1547718209,32.8
    // 2> sensor_6,1547718212,37.1
}
