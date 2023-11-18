package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import com.atguigu.util.DimUtil;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.JedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.zookeeper.Op;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @Author 城北徐公
 * @Date 2023/11/14-21:22
 */
public abstract class DimInfoRichMapFunctionAsync<T> extends RichAsyncFunction<T, T> {

    private AsyncConnection asyncConnection;
    private StatefulRedisConnection<String, String> redisConnection;
    private String tableName;

    /**
     * 构造器拿要关联的Hbase中维表的表名
     *
     * @param tableName
     */
    public DimInfoRichMapFunctionAsync(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncConnection = HBaseUtil.getAsyncConnection();
        redisConnection = JedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture.supplyAsync(new Supplier<JSONObject>() {
            @SneakyThrows
            @Override
            public JSONObject get() {
                //从redis里面查询维表数据
                RedisAsyncCommands<String, String> async = redisConnection.async();
                String pk = getPk(input);
                String redisKey = "DIM:" + tableName + ":" + pk;
                String dimInfoStr = async.get(redisKey).get();
                if (dimInfoStr != null) {
                    return JSON.parseObject(dimInfoStr);
                } else {
                    return null;
                }
            }
        }).thenApplyAsync(new Function<JSONObject, JSONObject>() {
            @SneakyThrows
            @Override
            public JSONObject apply(JSONObject jsonObject) {
                if (jsonObject != null){ //如果redis里面查到数据不为空,则直接返回
                    return jsonObject;
                }else {
                    //否则,去查询Hbase,并且写入到redis中去
                    String pk = getPk(input);
                    JSONObject dimInfoJson = HBaseUtil.getData(asyncConnection, Constant.HBASE_NAME_SPACE, tableName, pk);
                    JedisUtil.setData(redisConnection,tableName,pk,dimInfoJson.toJSONString());
                    return dimInfoJson;
                }
            }
        }).thenAccept(new Consumer<JSONObject     >() {
            @Override
            public void accept(JSONObject dimInfoJson) {
                //将维表数据补充上去
                join(input,dimInfoJson);
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("timeout>>>>>>>>>>>>>"+input);
    }

    protected abstract String getPk(T input);

    protected abstract void join(T input, JSONObject dimInfoJson);
}
