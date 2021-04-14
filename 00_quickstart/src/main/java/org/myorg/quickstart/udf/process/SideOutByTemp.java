package org.myorg.quickstart.udf.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.dto.SensorReading;

/**
 * @ClassName SideOutByTemp
 * @Description //分流温度在指定值30上下的数据
 * @Author ccy
 * @Date 2021/4/14 14:34
 * @Version 1.0
 **/
public class SideOutByTemp extends ProcessFunction<SensorReading, SensorReading> {

    private OutputTag<SensorReading> low;

    public SideOutByTemp(OutputTag<SensorReading> lowTemp) {
        low = lowTemp;
    }

    @Override
    //自定义处理类 分流30上下的数据
    public void processElement(SensorReading sr, Context ctx, Collector<SensorReading> out) throws Exception {
        if (sr.getTemperature() > 30) {
            out.collect(sr);
        } else {
            ctx.output(low, sr);
        }
    }
}