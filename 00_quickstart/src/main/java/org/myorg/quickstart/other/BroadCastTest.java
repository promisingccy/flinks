package org.myorg.quickstart.other;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @ClassName BroadCastTest
 * @Description //Broadcast广播变量 代码示例
 * @Author ccy
 * @Date 2021/3/18 11:35
 * @Version 1.0
 **/
public class BroadCastTest {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //第一步 封装一个DataSet-broadcast
        DataSet<Integer> broadcast = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");
        // 第二步 处理 data 过程中进行广播
        data.map(new RichMapFunction<String, String>() {
            private List list = new ArrayList();
            @Override
            public void open(Configuration parameters) throws Exception {
                //第四步 获取广播中名称为 number 的数据 加入到list中
                Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("number");
                list.addAll(broadcastSet);//此时值为 [1, 2, 3]
            }

            @Override
            public String map(String value) throws Exception {
                return value + ": "+ list;  //a: [1, 2, 3]   b: [1, 2, 3]
            }
        }).withBroadcastSet(broadcast, "number")  // 第三步 将 broadcast 变量（[1, 2, 3]）命名为 number 并广播出去
        .printToErr();//打印到err方便查看
    }
}
