package org.myorg.quickstart.util;

import org.apache.log4j.Logger;
import org.myorg.quickstart.dto.SensorReading;

/**
 * @ClassName MapUtil
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/14 13:47
 * @Version 1.0
 **/
public class MapUtil {
    public static Logger logger = Logger.getLogger(MapUtil.class);

    public static SensorReading map(String line) {
        logger.info(line);
        String[] fields = line.split(",");
        // logger.info(fields[0] + "-" + fields[1] + "-" + fields[2]);
        SensorReading sr = new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        logger.info(sr);
        return sr;
    }
}
