package com.atguigu.app.dwd.db;

import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/11/7-9:10
 * 互动域评论事务事实表
 * 任务:提取评论操作生成评论表，并将字典表中的相关维度退化到评论表中，写出到 Kafka 对应主题。
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd02_InteractionCommentInfo(lookUpJoin HBase|HDFS ZK) -> Kafka(ZK)
public class Dwd02_InteractionCommentInfo {
    public static void main(String[] args) throws Exception {

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
        tableEnv.executeSql(KafkaUtil.getTopicDbDDL("dwd02_comment_info_230524"));

        //3.过滤出评价表数据以及 创建视图
        Table commentInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "     `data`['id'] id,\n" +
                "     `data`['user_id'] user_id,\n" +
                "     `data`['nick_name'] nick_name,\n" +
                "     `data`['sku_id'] sku_id,\n" +
                "     `data`['spu_id'] spu_id,\n" +
                "     `data`['order_id'] order_id,\n" +
                "     `data`['appraise'] appraise,\n" +
                "     `data`['comment_txt'] comment_txt,\n" +
                "     `data`['create_time'] create_time,\n" +
                "     `data`['operate_time'] operate_time,\n" +
                "     `pt` pt\n" +
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='comment_info'\n" +
                "and (`type`= 'insert' or `type`='update')");
        tableEnv.createTemporaryView("comment_info",commentInfoTable);

        //4.读取hbase中的维表数据
        tableEnv.executeSql(HBaseUtil.getBaseDicDDL());

        //5.lookUpJoin关联评价表与base_dic维表     目的:做维度退化
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "     ci.id,\n" +
                "     ci.user_id,\n" +
                "     ci.nick_name,\n" +
                "     ci.sku_id,\n" +
                "     ci.spu_id,\n" +
                "     ci.order_id,\n" +
                "     ci.appraise,\n" +
                "     dim_base_dic.info.dic_name  appraise_name,\n" +
                "     ci.comment_txt,\n" +
                "     ci.create_time\n" +
                "from comment_info ci\n" +
                "join dim_base_dic  FOR SYSTEM_TIME AS OF ci.pt\n" +
                "on ci.appraise=dim_base_dic.rowkey");
        tableEnv.createTemporaryView("result_table",resultTable);

        //6.创建流表dwd_comment_info写入kafka的dwd_interaction_comment_info主题
        tableEnv.executeSql("" +
                "create table dwd_comment_info(\n" +
                "    `id` string,\n" +
                "    `user_id` string,\n" +
                "    `nick_name` string,\n" +
                "    `sku_id` string,\n" +
                "    `spu_id` string,\n" +
                "    `order_id` string,\n" +
                "    `appraise` string,\n" +
                "    `appraise_name` string,\n" +
                "    `comment_txt` string,\n" +
                "    `create_time` string\n" +
                ")"
                +KafkaUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        //7.查询写入
        tableEnv.executeSql("insert into dwd_comment_info select * from result_table")
                .print();

        //8.启动任务
        env.execute("Dwd02_InteractionCommentInfo");

    }
}
