package org.myorg.quickstart.udf.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.myorg.quickstart.dto.SensorReading;

/**
 * @ClassName MaxInGroup
 * @Description //获取相邻两条数据的最大温度
 * @Author ccy
 * @Date 2021/4/15 11:44
 * @Version 1.0
 **/
public class MaxInGroup implements ReduceFunction<SensorReading> {
    @Override
    public SensorReading reduce(SensorReading last, SensorReading current) throws Exception {
        return new SensorReading(last.getId(), current.getTimestamp(), Math.max(last.getTemperature(), current.getTemperature()));
    }
}
