package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @Author 城北徐公
 * @Date 2023/11/15-8:30
 */
public class DimUtil {
    /**
     * 查询维度信息:先从redis里面查询需要的维度数据,如果有直接返回,如果没有则查询hbase,并且写到redis中,
     * tops：如果hbase中的维度数据更新(删除),redis的数据应同步更新(删除),防止数据不一致
     *
     * @param connection hbase连接
     * @param jedis      redis连接
     * @param TableName  表名
     * @param pk         (hbase中的rowKey,每次查询读出一行数据) 作为hbase的主键
     *                   ("DIM:" + tableName + ":" + pk) 作为redis的主键
     * @return 从hbase或者redis里面查询到到的维度数据
     * 旁路缓存技术
     */
    public static JSONObject getDimInfo(Connection connection, Jedis jedis, String TableName, String pk) throws IOException {

        //1.先查redis数据
        String redisKey = "DIM:" + TableName + ":" + pk;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {
            //TTL读更新(热点数据,重新刷写生命周期)
            jedis.expire(redisKey, Constant.ONE_DAY);
            return JSON.parseObject(dimInfoStr);
        }

        //2.如果redis里面没有,则去查询hbase
        JSONObject dimInfo = HBaseUtil.getData(connection, Constant.HBASE_NAME_SPACE, TableName, pk);

        //3.同时将热点数据写进redis(多列,存储到redis中时转为JSONObject就变成一条数据了),并重新刷写生命周期
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, Constant.ONE_DAY);

        //4.结果返回
        return dimInfo;

    }
}
