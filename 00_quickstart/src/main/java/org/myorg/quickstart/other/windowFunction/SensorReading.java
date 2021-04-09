package org.myorg.quickstart.other.windowFunction;

/**
 * @ClassName SensorReading
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/8 16:45
 * @Version 1.0
 **/
public class SensorReading {
    public String key;
    public Long time;
    public Integer value;

    public SensorReading(String key, Integer value, Long time){
        this.key = key;
        this.value = value;
        this.time = time;
    }

}
