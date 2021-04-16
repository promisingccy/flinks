package org.myorg.quickstart.other.FTable;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName Base1Usage
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/15 17:51
 * @Version 1.0
 **/
public class Base1Usage {

    public static void main(String[] args) throws Exception {
        // 1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 基于老版本 planner 的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);


        // 1.3 基于 Blink 的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);


        // 1.4 基于 Blink 的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableENv = TableEnvironment.create(blinkBatchSettings);

        // 2、表的创建：连接外部系统，读取数据
        String filePath = "src/main/resources/sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();
        // root
        //  |-- id: STRING
        //  |-- timestamp: BIGINT
        //  |-- temp: DOUBLE
        tableEnv.toAppendStream(inputTable, Row.class).print("tableEnv");
        // tableEnv> sensor_1,1547718199,35.8
        // tableEnv> sensor_6,1547718201,15.4
        // tableEnv> sensor_7,1547718202,6.7
        // tableEnv> sensor_10,1547718205,38.1
        // tableEnv> sensor_2,1547718207,36.3
        // tableEnv> sensor_4,1547718209,32.8
        // tableEnv> sensor_6,1547718212,37.1
        // tableEnv> sensor_4,1547718215,34.1
        // tableEnv> sensor_4,1547718218,11.89
        // tableEnv> sensor_10,1547718220,32.1
        // tableEnv> sensor_7,1547718223,33.6
        // tableEnv> sensor_6,1547718225,23.22

        // 3、查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                .filter("id = 'sensor_6'");

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        // result> sensor_6,15.4
        // result> sensor_6,37.1
        // result> sensor_6,23.22

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        // agg> (true,sensor_1,1,35.8)
        // agg> (true,sensor_6,1,15.4)
        // agg> (true,sensor_7,1,6.7)
        // agg> (true,sensor_10,1,38.1)
        // agg> (true,sensor_2,1,36.3)
        // agg> (true,sensor_4,1,32.8)
        // agg> (false,sensor_6,1,15.4)
        // agg> (true,sensor_6,2,26.25)
        // agg> (false,sensor_4,1,32.8)
        // agg> (true,sensor_4,2,33.45)
        // agg> (false,sensor_4,2,33.45)
        // agg> (true,sensor_4,3,26.263333333333335)
        // agg> (false,sensor_10,1,38.1)
        // agg> (true,sensor_10,2,35.1)
        // agg> (false,sensor_7,1,6.7)
        // agg> (true,sensor_7,2,20.150000000000002)
        // agg> (false,sensor_6,2,26.25)
        // agg> (true,sensor_6,3,25.24)

        // 3.2 SQL
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAgg");
        // sqlAgg> (true,sensor_1,1,35.8)
        // sqlAgg> (true,sensor_6,1,15.4)
        // sqlAgg> (true,sensor_7,1,6.7)
        // sqlAgg> (true,sensor_10,1,38.1)
        // sqlAgg> (true,sensor_2,1,36.3)
        // sqlAgg> (true,sensor_4,1,32.8)
        // sqlAgg> (false,sensor_6,1,15.4)
        // sqlAgg> (true,sensor_6,2,26.25)
        // sqlAgg> (false,sensor_4,1,32.8)
        // sqlAgg> (true,sensor_4,2,33.45)
        // sqlAgg> (false,sensor_4,2,33.45)
        // sqlAgg> (true,sensor_4,3,26.263333333333335)
        // sqlAgg> (false,sensor_10,1,38.1)
        // sqlAgg> (true,sensor_10,2,35.1)
        // sqlAgg> (false,sensor_7,1,6.7)
        // sqlAgg> (true,sensor_7,2,20.150000000000002)
        // sqlAgg> (false,sensor_6,2,26.25)
        // sqlAgg> (true,sensor_6,3,25.24)

        env.execute();
    }
}
