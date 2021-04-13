package org.myorg.quickstart.other.CProcess;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.dto.SensorReading;

/**
 * @ClassName KeyedProcess
 * @Description //实现：监控温度传感器的温度值，如果温度值在 10 秒钟之内连续上升，则报警。
 * @Author ccy
 * @Date 2021/4/13 16:10
 * @Version 1.0
 **/
public class KeyedProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从socket文件读取
        //在 10.0.81.88 上执行 nc -l 33335
        DataStream<String> source = env.socketTextStream("10.0.81.88", 33335);

        //转换数据类型
        DataStream<SensorReading> stream = source.map(KeyedProcess::map);

        stream.keyBy("id")
            .process(new MyProcess(10))
            .print();
        // 传感器 sensor_7 温度值连续 10 秒上升

        env.execute();
    }

    // 实现自定义的处理函数
    // 监控温度传感器的温度值，如果温度值在 10 秒钟之内连续上升，则输出报警
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, String>{
        //定义多久之内连续上升
        private Integer interval;
        //存储时间定时器状态
        ValueState<Long> tsTimerState;
        //存储上一次的温度
        ValueState<Double> lastTempState;

        public MyProcess(Integer interval){
            this.interval = interval;
        }

        @Override
        //初始化获取状态变量
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading sr, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long tsTimer = tsTimerState.value();

            double curTemp = sr.getTemperature();
            if(null != lastTemp){//非首个元素
                // 如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
                if (curTemp > lastTemp && tsTimer == null) {
                    // 计算出定时器时间戳
                    Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);// 注册定时器
                    tsTimerState.update(ts);// 更新定时器
                } else if (curTemp < lastTemp && tsTimer != null) {
                    // 如果温度下降，那么删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(tsTimer);
                    tsTimerState.clear();
                }
            }

            // 更新温度状态
            lastTempState.update(curTemp);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出报警信息
            Object field = ctx.getCurrentKey().getField(0);
            String msg = String.format("传感器 %s 温度值连续 %d 秒上升", field, interval);
            out.collect(msg);

        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
            lastTempState.clear();

        }
    }


    private static SensorReading map(String line) {
        // System.out.println(line);
        String[] fields = line.split(",");
        // System.out.println(fields[0] + "-" + fields[1] + "-" + fields[2]);
        SensorReading sr = new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        // System.out.println(sr);
        return sr;
    }
}
