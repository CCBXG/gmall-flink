package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/11/4-15:12
 */
public class DimTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess,JSONObject> {
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

    }

    @Override
    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

    }
}
