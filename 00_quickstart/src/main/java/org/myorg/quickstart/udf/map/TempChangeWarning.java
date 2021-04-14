package org.myorg.quickstart.udf.map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.dto.SensorReading;

/**
 * @ClassName TempChangeWarning
 * @Description //如果连续的两个温度差值超过 10 度，就输出报警。
 * @Author ccy
 * @Date 2021/4/14 13:50
 * @Version 1.0
 **/
public class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

    //存储温度跳变阈值
    private Double threshold;

    //构造参数接收阈值
    public TempChangeWarning(Double threshold){
        this.threshold = threshold;
    }


    /**状态变量**/
    //存储上一次的温度
    private ValueState<Double> lastTemp;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
    }

    @Override
    public void flatMap(SensorReading sr, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        Double last = lastTemp.value();
        if(null != last){
            double diff = Math.abs(sr.getTemperature() - last);
            if(diff >= threshold){
                //输出格式为 id/上一次温度/当前温度
                out.collect(new Tuple3<>(sr.getId(), last, sr.getTemperature()));
            }
        }
        //更新状态
        lastTemp.update(sr.getTemperature());
    }

    @Override
    public void close() throws Exception {
        lastTemp.clear();
    }
}
