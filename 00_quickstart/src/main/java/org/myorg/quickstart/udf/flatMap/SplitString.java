package org.myorg.quickstart.udf.flatMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName SplitString
 * @Description //将来源的字符串按照空格分隔成数组，对数组的每一个元素进行输出 Tuple2<>(word, 1)
 * @Author ccy
 * @Date 2021/4/15 10:36
 * @Version 1.0
 **/
public class SplitString implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private String split;

    public SplitString(String split){
        this.split = split;
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.split(split);
        for (String word : words) {
            out.collect(new Tuple2<>(word, 1));
        }
    }
}
