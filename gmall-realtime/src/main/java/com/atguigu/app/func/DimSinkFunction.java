package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.JedisUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * 向hbase中写入数据
 * @Author 城北徐公
 * @Date 2023/11/5-21:07
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = JedisUtil.getJedis();
    }

    /*   value数据样式(数据流中已经过滤过的数据)
   {
       "database": "gmall-220623-flink",
           "table": "comment_info",
           "type": "insert",
           "ts": 1669162958,
           "xid": 1111,
           "xoffset": 13941,
           "data": {
               "id": 1595211185799847960,
               "user_id": 119
           },
           "sink_table":"dim_comment_info",
           "row_key_column":"id",
           "sink_extend":null,
           "family":"info"
      }
 */
    //value:已经过滤过的kafka中的数据    (只要我们MySQL配置表中需要的字段)

    /**
     * 向hbase中写入数据(已经过滤过的)
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //写入到hbase需要准备   connect, nameSpace:tableName, rowKey/Extend+rowKey, family, data
        String type = value.getString("type");
        String sinkTable = value.getString("sink_table");
        String sinkExtend = value.getString("sink_extend");
        String columnFamily = value.getString("family");
        String rowKeyColumn = value.getString("row_key_column"); //用data中的哪个字段做rowKey
        JSONObject data = value.getJSONObject("data");
        //获取rowKey
        String rowKey = data.getString(rowKeyColumn);
        if (sinkExtend != null){
            rowKey = HBaseUtil.getRowKey(rowKey, sinkExtend);
        }
        //向Hbase中插入数据或删除数据
        if ("delete".equals(type)){
            HBaseUtil.deleteData(connection, Constant.HBASE_NAME_SPACE, sinkTable, rowKey);
            JedisUtil.deleteData(jedis,sinkTable,rowKey);//如果hbase里面删除数据,redis里面同步删除
        }else {
            data.remove(rowKeyColumn);
            //如果为更新操作,则将更新数据直接同步到Redis中  保证Redis与HBase数据存储的一致性
            if ("update".equals(type)) {
                JedisUtil.setData(jedis, sinkTable, rowKey, data.toJSONString());
            }
            HBaseUtil.putJsonData(connection, Constant.HBASE_NAME_SPACE, sinkTable, rowKey, columnFamily, data );
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
