package com.atguigu.util;

import com.atguigu.common.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author 城北徐公
 * @Date 2023/11/14-20:53
 */
public class JedisUtil {
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
    }

    /**
     * 获取jedis客户端连接
     * @return
     */
    public static Jedis getJedis(){
        System.out.println("==获取Jedis客户端==");
        return jedisPool.getResource();
    }

    /**
     * 获取到一个 redis 线程安全的异步连接, key value 都用 utf-8 进行编码
     * @return
     */
    public static StatefulRedisConnection<String,String> getAsyncRedisConnection(){
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379");
        System.out.println("==获取AsyncJedis客户端==");
        return redisClient.connect();
    }

    /**
     * 向rides中写入数据,(数据结构为String,主键为"DIM:"+TableName+":"+pk)
     * @param jedis     jedis客户端连接
     * @param TableName 表名
     * @param pk        主键
     * @param value     要写入rides的值
     */
    public static void setData(Jedis jedis, String TableName, String pk, String value){
        String redisKey="DIM:"+TableName+":"+pk;
        //向redis里面写入数据,设置有效期为一天
        jedis.setex(redisKey, Constant.ONE_DAY,value);
    }

    /**
     * Async异步向redis里面写入数据
     * @param redisConnection  异步连接
     * @param TableName        表名
     * @param pk               主键
     * @param value            要写入的值
     */
    public static void setData( StatefulRedisConnection<String,String> redisConnection, String TableName, String pk, String value ){
        String redisKey="DIM:"+TableName+":"+pk;
        RedisAsyncCommands<String, String> async = redisConnection.async();
        async.setex(redisKey,Constant.ONE_DAY,value);
    }

    /**
     * 通过redisKey删除一条数据,(数据结构为String,主键为"DIM:"+TableName+":"+pk)
     * @param jedis      redis连接
     * @param TableName  表名
     * @param pk         主键
     */
    public static void deleteData(Jedis jedis, String TableName, String pk){
        String redisKey="DIM:"+TableName+":"+pk;
        jedis.del(redisKey);
    }
}
