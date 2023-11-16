package com.atguigu.app.dwd.db;

import com.atguigu.common.Constant;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/11/7-16:20
 * 交易域加购事实表
 * 任务:提取加购操作生成加购表，写出到 Kafka 对应主题。
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd03_TradeCartAdd -> Kafka(ZK)
public class Dwd03_TradeCartAdd {
    public static void main(String[] args) {
        //1.获取执行环境 FlinkSQL
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

       /* //todo 生产环境要开启chink point
        env.enableCheckpointing(60000*5L);  //5分钟做一次
        env.setStateBackend(new HashMapStateBackend()); //状态存储方式
        //chink point 相关设置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-ck"); //存储地址
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); //Cancel任务时保存最后一次CheckPoint结果
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L); //
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L)); //设置重启策略
        */

        //2.读取kafka中topic_db数据并创建动态表
        tableEnv.executeSql(KafkaUtil.getTopicDbDDL("Dwd03_TradeCartAdd"));
        //tableEnv.sqlQuery("select * from ods_topic_db").execute().print();

        //3.过滤出交易域加购事实表
        Table cartInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "     `data`['id'] id,\n" +
                "     `data`['user_id'] user_id,\n" +
                "     `data`['sku_id'] sku_id,\n" +
                "     `data`['cart_price'] cart_price,\n" +
                "     if(`type` = 'insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,\n" +
                "     `data`['sku_name'] sku_name,\n" +
                "     `data`['is_checked'] is_checked,\n" +
                "     `data`['create_time'] create_time,\n" +
                "     `data`['operate_time'] operate_time,\n" +
                "     `data`['is_ordered'] is_ordered,\n" +
                "     `data`['order_time'] order_time,\n" +
                "     `data`['source_type'] source_type,\n" +
                "     `data`['source_id'] source_id\n" +
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='cart_info'\n" +
                "and (`type`= 'insert' or (`type`= 'update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as int)>cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_info",cartInfoTable);
        //cartInfoTable.execute().print();

        //4.构建kafka Sink Table
        tableEnv.executeSql("" +
                "create table dwd_cart_info(\n" +
                "    `id` string,\n" +
                "    `user_id` string,\n" +
                "    `sku_id` string,\n" +
                "    `cart_price` string,\n" +
                "    `sku_num` string,\n" +
                "    `sku_name` string,\n" +
                "    `is_checked` string,\n" +
                "    `create_time` string,\n" +
                "    `operate_time` string,\n" +
                "    `is_ordered` string,\n" +
                "    `order_time` string,\n" +
                "    `source_type` string,\n" +
                "    `source_id` string\n" +
                ")"+KafkaUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        //5.数据写出
        tableEnv.executeSql("insert into  dwd_cart_info select * from cart_info");


    }
}
