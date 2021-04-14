package org.myorg.quickstart.udf.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.dto.SensorReading;

/**
 * @ClassName ContinueUpTemp
 * @Description //监控温度传感器的温度值，如果温度值在 10 秒钟之内连续上升，则输出报警
 * @Author ccy
 * @Date 2021/4/14 19:49
 * @Version 1.0
 **/
public class ContinueUpTemp extends KeyedProcessFunction<Tuple, SensorReading, String> {
    //定义多久之内连续上升
    private Integer interval;
    //存储时间定时器状态
    ValueState<Long> tsTimerState;
    //存储上一次的温度
    ValueState<Double> lastTempState;

    public ContinueUpTemp(Integer interval){
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