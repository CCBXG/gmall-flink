package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

/**
 * @Author 城北徐公
 * @Date 2023/11/8-14:08
 */
public class DwdTableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;
    private HashMap<String,TableProcess> mapStateDescriptorMap;

    public DwdTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor=mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mapStateDescriptorMap = new HashMap<>();
        Connection connection = DriverManager.getConnection(Constant.MYSQL_URL, "root", "000000");
        List<TableProcess> queryList = JdbcUtil.queryList(connection, "select * from `gmall_config`.table_process where sink_type='dwd'", TableProcess.class, true);
        for (TableProcess tableProcess : queryList) {
            String sourceTable = tableProcess.getSourceTable();
            String sourceType = tableProcess.getSourceType();
            String key = sourceTable + "_" + sourceType;
            mapStateDescriptorMap.put(key,tableProcess);
        }
        connection.close();
    }

    //value:{"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        String sourceTable = value.getString("table");
        String sourceType = value.getString("type");
        String key = sourceTable + "_" + sourceType;
        //根据key取出两个状态中和预加载map中的配置信息
        TableProcess tableProcess = ctx.getBroadcastState(mapStateDescriptor).get(key);
        TableProcess tableProcessMap = mapStateDescriptorMap.get(key);

        //数据过滤  行过滤
        if (tableProcessMap!=null || tableProcess!=null){
            if (tableProcess ==null){
                tableProcess=tableProcessMap;
            }
            //列过滤
            filterColumns(value.getJSONObject("data"),tableProcess.getSinkColumns());
            //补充信息
            value.put("sink_topic",tableProcess.getSinkTable());
            //写出
            out.collect(value);
        }else {
            System.out.println(sourceTable+"表不是事实表数据或操作类型不匹配"+sourceType);
        }

    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //获取操作类型
        JSONObject jsonObject = JSON.parseObject(value);
        System.out.println(jsonObject);      //flinkCDC监控到的数据原样
        String op = jsonObject.getString("op");

        //根据操作类型,删除或新增状态数据
        if ("d".equals(op)){
            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);
            String key = tableProcess.getSourceTable() + "_" + tableProcess.getSourceType();
            broadcastState.remove(key);
            mapStateDescriptorMap.remove(key);
        }else {
            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
            String key = tableProcess.getSourceTable() + "_" + tableProcess.getSourceType();
            broadcastState.put(key,tableProcess);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {

        String[] split = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(split);

//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnsList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnsList.contains(next.getKey()));
    }

}
