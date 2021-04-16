package org.myorg.quickstart.other.FTable;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.myorg.quickstart.dto.SensorReading;
import org.myorg.quickstart.util.SourceUtil;

/**
 * @ClassName BaseUsage
 * @Description //table基础用法
 * @Author ccy
 * @Date 2021/4/15 17:39
 * @Version 1.0
 **/
public class BaseUsage {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> source = env.addSource(new SourceUtil(), "sensor");

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(source);

        Table resultTable = dataTable.select("id, temperature")
                .where("id = 'sensor_1'");

        tableEnv.createTemporaryView("sensor", resultTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();

        // result> sensor_1,5.5
        // sql> sensor_1,5.5
    }


}
