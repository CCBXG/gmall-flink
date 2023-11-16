package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

/**
 * 1.预加载配置表信息
 * 2.控制数据该去往哪张hbase表
 * 3.过滤出kafka里面需要写入hbase中的数据
 *
 * @Author 城北徐公
 * @Date 2023/11/4-15:12
 */
public class DimTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess, JSONObject> {

    //状态是为了控制数据流中的数据该不该去往hbase的dim层
    //状态中存储的是  key：sourceTable(在hbase创建的表)  value：TableProcess(建表所需的信息)
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private HashMap<String, TableProcess> tableProcessHashMap;

    /**
     * 创建mapStateDescriptor
     * @param mapStateDescriptor 资源复用:构造器拿到已经创建过的mapStateDescriptor
     */
    public DimTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 预加载配置表信息,提前读一遍MySQL表中的配置信息,并且建表,防止没有状态时,到来的数据丢失
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        tableProcessHashMap = new HashMap<>();
        //获取连接
        Connection mysqlConn = DriverManager.getConnection(Constant.MYSQL_URL, "root", "000000");
        org.apache.hadoop.hbase.client.Connection hbaseConn = HBaseUtil.getConnection();
        //预加载配置信息,从MySQL查询一次
        List<TableProcess> queryList = JdbcUtil.queryList(mysqlConn, "select * from `gmall_config`.table_process where sink_type='dim'", TableProcess.class, true);
        for (TableProcess tableProcess : queryList) {
            System.out.println(tableProcess);
            byte[][] splitKeys = HBaseUtil.getSplitKeys(tableProcess.getSinkExtend());
            HBaseUtil.createTable(hbaseConn, Constant.HBASE_NAME_SPACE, tableProcess.getSinkTable(), splitKeys, tableProcess.getSinkFamily());
            tableProcessHashMap.put(tableProcess.getSourceTable(), tableProcess);
        }

        //关闭连接
        mysqlConn.close();
        hbaseConn.close();

    }

    /*   value数据样式(数据流中的数据)
        {
            "database": "gmall-220623-flink",
                "table": "comment_info",
                "type": "insert",
                "ts": 1669162958,
                "xid": 1111,
                "xoffset": 13941,
                "data": {
                    "id": 1595211185799847960,
                    "user_id": 119,
                    "nick_name": null,
                    "head_img": null,
                    "sku_id": 31,
                    "spu_id": 10,
                    "order_id": 987,
                    "appraise": "1204",
                    "comment_txt": "评论内容：48384811984748167197482849234338563286217912223261",
                    "create_time": "2022-08-02 08:22:38",
                    "operate_time": null
                }
           }
      */
    //这个value是kafka数据流里面的数据
    /**
     * 对kafka来的数据进行处理,对于kafka数据只要配置表中有的表项,以及过滤出data中需要写进hbase的数据
     * @param value The stream element.
     * @param ctx   A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *              querying the current processing/event time and updating the broadcast state. The context
     *              is only valid during the invocation of this method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取广播状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = value.getString("table"); //kafka数据里面的sourceTable
        String type = value.getString("type");   //kafka数据里面的类型
        TableProcess tableProcessMap = tableProcessHashMap.get(table); //预加载的配置表
        TableProcess tableProcess = broadcastState.get(table); //MySQL中的配置表
        //行过滤    根据状态中的数据做过滤  (判断table在不在状态中(或者预加载表中)，以及状态是否为insert)
        if ((tableProcessMap != null || tableProcess != null) && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
            if (tableProcess == null) {
                tableProcess = tableProcessMap;
            }
            //列过滤      (目的:拿到data里面需要的数据  将其存储到hbase中去)
            filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //写入未来需要的数据(向hbase中插入数据时需要的字段)
            value.put("sink_table", tableProcess.getSinkTable());
            value.put("sink_extend", tableProcess.getSinkExtend());
            value.put("row_key_column", tableProcess.getSinkRowKey());
            value.put("family", tableProcess.getSinkFamily());

            out.collect(value);
        } else {
            if (tableProcess == null) {
                System.out.println(table + "不是维表");
            } else {
                System.out.println("操作类型不匹配" + type);
            }
        }
    }

    /*
    {
            "before": null,
            "after": {
                    "source_table": "base_category3",
                    "sink_table": "dim_base_category3",
                    "sink_columns": "id,name,category2_id",
                    "sink_pk": "id",
                    "sink_extend": null
            },
            "source": {
                    "version": "1.5.4.Final",
                    "connector": "mysql",
                    "name": "mysql_binlog_source",
                    "ts_ms": 1669162876406,
                    "snapshot": "false",
                    "db": "gmall-220623-config",
                    "sequence": null,
                    "table": "table_process",
                    "server_id": 0,
                    "gtid": null,
                    "file": "",
                    "pos": 0,
                    "row": 0,
                    "thread": null,
                    "query": null
           },
           "op": "r",
           "ts_ms": 1669162876406,
           "transaction": null
    }
     */
    //这个value是广播流里面的数据
    /**
     * 加载mysql中的表配置信息,加载进状态中
     * @param value The stream element.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
     *              current processing/event time and updating the broadcast state. The context is only valid
     *              during the invocation of this method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取sink_type(对于数据流中的数据，只要dim 层的)
        String sinkType = value.getSinkType();
        if ("dim".equals(sinkType)) {
            //获取op字段(将建的表放入状态中,用于过滤kafka来的数据)
            String op = value.getOp();
            //获取sourceTable(用于当作状态中的key)
            String sourceTable = value.getSourceTable();
            if ("d".equals(op)) { //说明该维表库已经被删除,kafka中该表的后续数据不必再收集
                broadcastState.remove(sourceTable);
            } else {
                //拿sourceTable做key的原因:为了因为kafka数据中携带的表信息为sourceTable
                broadcastState.put(sourceTable, value);
            }
        }
    }

    /**
     * 列过滤方法
     * @param data        kafka来的数据
     * @param sinkColumns 要写入hbase表中的列   (该字段在配置表中已经写好了)
     */
    private void filterColumns(JSONObject data, String sinkColumns) {
        //1.先把配置表中   需要写入hbase的字段拿出来 (为了方便遍历,将数组转为list集合)
        String[] split = sinkColumns.split(",");
        List<String> writerColumns = (List<String>) Arrays.asList(split);

        //2.拿到原始数据data里面的字段
        Set<Map.Entry<String, Object>> entries = data.entrySet();

        //3.把data里面的字段 与 配置表中需要写入hbase的数据一一比较，删掉data中的其他字段
        entries.removeIf(next -> !writerColumns.contains(next.getKey()));

    }
}
