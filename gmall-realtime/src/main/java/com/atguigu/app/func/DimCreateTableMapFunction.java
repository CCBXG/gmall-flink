package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Author 城北徐公
 * @Date 2023/11/3-21:14
 * 根据数据类型将数据转为javabean，还要负责删表，建表（故将删表建表操作写入工具类中）
 */
public class DimCreateTableMapFunction extends RichMapFunction<String, TableProcess> {

    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
    }

    @Override
    public TableProcess map(String value) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        //获取操作数字段
        String op = jsonObject.getString("op");
        TableProcess tableProcess;
        if ("d".equals(op)){
            tableProcess = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);
        } else {
            tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
        }

        if ("dim".equals(tableProcess.getSinkType())) {
            //建表 删表
            if ("d".equals(op)) {
                HBaseUtil.dropTable(connection, Constant.HBASE_NAME_SPACE, tableProcess.getSinkTable());
            } else if ("u".equals(op)) {
                HBaseUtil.dropTable(connection, Constant.HBASE_NAME_SPACE, tableProcess.getSinkTable());
                byte[][] splitKeys = HBaseUtil.getSplitKeys(tableProcess.getSinkExtend());
                HBaseUtil.createTable(connection, Constant.HBASE_NAME_SPACE, tableProcess.getSinkTable(), splitKeys, tableProcess.getSinkFamily());
            } else {
                byte[][] splitKeys = HBaseUtil.getSplitKeys(tableProcess.getSinkExtend());
                HBaseUtil.createTable(connection, Constant.HBASE_NAME_SPACE, tableProcess.getSinkTable(), splitKeys, tableProcess.getSinkFamily());
            }
        }

        return tableProcess;
    }



    @Override
    public void close() throws Exception {
        connection.close();
    }
}
