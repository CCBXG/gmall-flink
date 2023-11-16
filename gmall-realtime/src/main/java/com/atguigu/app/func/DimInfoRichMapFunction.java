package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.DimUtil;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.JedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @Author 城北徐公
 * @Date 2023/11/14-21:22
 */
public abstract class DimInfoRichMapFunction<T> extends RichMapFunction<T, T> {
    private Connection connection;
    private Jedis jedis;
    private String tableName;

    //构造器拿到tableName(该tableName为你要查询的维度表的表名)
    public DimInfoRichMapFunction(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 初始化获取到hbase和redis连接
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = JedisUtil.getJedis();
    }

    /**
     * @param value 想要从hbase(redis)中拿到的数据 的Qualifier(key)
     * @return
     */
    public abstract String getPk(T value);

    /**
     * @param value   你自己的数据
     * @param dimInfo 查出来的维度数据
     */
    protected abstract void join(T value, JSONObject dimInfo);


    //通过传入的pk查询到一行维度信息,将这行维度信息选择性的补充进自己的数据中
    @Override
    public T map(T value) throws Exception {
        //1.获得主键
        String pk = getPk(value);

        //System.out.println("mapFun----->" + pk);
        //2.根据pk查询一行维度信息
        JSONObject dimInfo = DimUtil.getDimInfo(connection, jedis, tableName, pk);

        //3.向T对象设置维度信息
        join(value, dimInfo);

        return value;
    }

    @Override
    public void close() throws Exception {
        connection.close();
        jedis.close();
    }
}
