package org.myorg.quickstart.udf.filter;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @ClassName StartWith
 * @Description //TODO
 * @Author ccy
 * @Date 2021/4/15 11:04
 * @Version 1.0
 **/
public class StartWith implements FilterFunction<String> {

    private String pre;

    public StartWith(String pre){
        this.pre = pre;
    }

    @Override
    public boolean filter(String s) throws Exception {
        return s.startsWith(pre);
    }
}
