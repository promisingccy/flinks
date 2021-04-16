package org.myorg.quickstart.udf.map;

import org.apache.flink.api.common.functions.MapFunction;


/**
 * @ClassName GetStrLength
 * @Description // 输入：str  输出：str的长度
 * @Author ccy
 * @Date 2021/4/15 10:59
 * @Version 1.0
 **/
public class GetStrLength implements MapFunction<String, Integer> {
    @Override
    public Integer map(String s) throws Exception {
        return s.length();
    }
}
