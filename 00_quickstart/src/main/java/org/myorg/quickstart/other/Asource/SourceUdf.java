package org.myorg.quickstart.other.Asource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.myorg.quickstart.dto.SensorReading;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName SourceUdf
 * @Description //实现自定义的 SourceFunction
 * @Author ccy
 * @Date 2021/4/13 14:23
 * @Version 1.0
 **/
public class SourceUdf {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> stream = env.addSource(new MySensorSource());
        stream.print();

        env.execute();
        // 一次while的输出
        // 1> SensorReading{id='sensor_10', timestamp=1618297245731, temperature=21.341399835605632}
        // 2> SensorReading{id='sensor_4', timestamp=1618297245731, temperature=44.87828215666852}
        // 1> SensorReading{id='sensor_7', timestamp=1618297245731, temperature=38.170145636233656}
        // 3> SensorReading{id='sensor_1', timestamp=1618297245731, temperature=90.18652845933048}
        // 1> SensorReading{id='sensor_9', timestamp=1618297245732, temperature=56.50621266502344}
        // 3> SensorReading{id='sensor_5', timestamp=1618297245731, temperature=88.06643764998466}
        // 2> SensorReading{id='sensor_8', timestamp=1618297245731, temperature=54.92955496186283}
        // 4> SensorReading{id='sensor_3', timestamp=1618297245731, temperature=92.39707844932808}
        // 4> SensorReading{id='sensor_2', timestamp=1618297245731, temperature=-7.950693210358991}
        // 4> SensorReading{id='sensor_6', timestamp=1618297245732, temperature=53.07421411500475}
    }

    // 实现自定义的 SourceFunction，用于随机生成传感器数
    public static class MySensorSource implements SourceFunction<SensorReading>{

        //控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> out) throws Exception {
            Random random = new Random();

            HashMap<String, Double> map = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                map.put("sensor_" + (i + 1), 60+random.nextGaussian()*30);
            }

            while (running){
                for (String sid : map.keySet()) {
                    double newTemp = map.get(sid) + random.nextGaussian();
                    out.collect(new SensorReading(sid, System.currentTimeMillis(), newTemp));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
