package org.myorg.quickstart.other.DTransform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.util.MapUtil;
import org.myorg.quickstart.util.SourceUtil;

/**
 * @ClassName MultiStream
 * @Description //单一流 与 多个流 的转换
 * @Author ccy
 * @Date 2021/4/15 15:20
 * @Version 1.0
 **/
public class MultiStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        // DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");
        // 转换成 SensorReading 类型
        // DataStream<SensorReading> dataStream = inputStream.map(MapUtil::map);
        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceUtil(), "sensor");


        // 1、分流，按照温度值30度为界分为两流
        final OutputTag<SensorReading> highTag = new OutputTag<>("high") {};
        final OutputTag<SensorReading> lowTag = new OutputTag<>("low") {};
        final OutputTag<SensorReading> normalTag = new OutputTag<>("normal") {};
        SingleOutputStreamOperator<Object> process = dataStream.process(new ProcessFunction<>() {
            @Override
            public void processElement(SensorReading sr, Context ctx, Collector<Object> out) throws Exception {
                //如果温度大于 30 度，返回 high，否则返回 low
                if (sr.getTemperature() > 30) {
                    ctx.output(highTag, sr);
                } else if(sr.getTemperature() == 30) {
                    ctx.output(normalTag, sr);
                }else{
                    ctx.output(lowTag, sr);
                }
            }
        });

        DataStream<SensorReading> highStream = process.getSideOutput(highTag);
        DataStream<SensorReading> lowStream = process.getSideOutput(lowTag);
        DataStream<SensorReading> normalSteam = process.getSideOutput(normalTag);

        highStream.print("highStream");
        normalSteam.print("normalSteam");
        lowStream.print("lowStream");

        // 3、union 联合多条流
        // highStream.union(lowStream, normalSteam).print("union");

        env.execute();

        // lowStream> SensorReading{id='sensor_5', timestamp=1618479382436, temperature=27.5}
        // lowStream> SensorReading{id='sensor_5', timestamp=1618479383475, temperature=27.5}
        // highStream> SensorReading{id='sensor_9', timestamp=1618479384475, temperature=49.5}
        // highStream> SensorReading{id='sensor_9', timestamp=1618479385477, temperature=49.5}
        // highStream> SensorReading{id='sensor_8', timestamp=1618479386477, temperature=44.0}
        // lowStream> SensorReading{id='sensor_3', timestamp=1618479387477, temperature=16.5}
        // lowStream> SensorReading{id='sensor_4', timestamp=1618479388478, temperature=22.0}
        // lowStream> SensorReading{id='sensor_2', timestamp=1618479389478, temperature=11.0}
        // highStream> SensorReading{id='sensor_8', timestamp=1618479390479, temperature=44.0}
    }
}
