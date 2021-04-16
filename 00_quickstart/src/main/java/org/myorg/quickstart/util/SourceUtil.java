package org.myorg.quickstart.util;

import cn.hutool.core.date.SystemClock;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.myorg.quickstart.dto.SensorReading;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName SourceUtil
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/15 16:59
 * @Version 1.0
 **/
public class SourceUtil implements SourceFunction<SensorReading> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {

        while (isRunning){
            TimeUnit.SECONDS.sleep(1);
            int i = random.nextInt(10);
            ctx.collect(new SensorReading("sensor_"+i, SystemClock.now(), i*5.5));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
