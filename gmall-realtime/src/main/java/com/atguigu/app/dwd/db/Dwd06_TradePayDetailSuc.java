package com.atguigu.app.dwd.db;

import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/11/8-9:54
 * 交易域支付成功事务事实表
 * 从 Kafka topic_db主题筛选支付成功数据(payment_info)、从dwd_trade_order_detail主题中读取订单事实数据、
 * HBase-LookUp字典表，关联三张表形成支付成功宽表，写入 Kafka 支付成功主题。
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS)/Kafka(DWD)/Hbase(DIM) -> FlinkApp -> Kafka(DWD)
//程序流:
//                         -> |  (Kafka(ZK))ODS                            |
//      Mock -> maxwell.sh -> |  (Kafka(ZK) -> Dwd04_TradeOrderDetail)DWD  | -> Dwd06_TradePayDetailSuc ->  Kafka(ZK)
//                         -> |  (DimApp -> HBase(HDFS ZK))DIM             |
public class Dwd06_TradePayDetailSuc {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境 FlinkSQL
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置TTL
        TableConfig config = tableEnv.getConfig();
        //4张表的数据同时生成,给定的时间只需要考虑生产延迟即可
        //config.setIdleStateRetention(Duration.ofSeconds(10));

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
        tableEnv.executeSql(KafkaUtil.getTopicDbDDL("Dwd05_TradeOrderCancelDetail"));

        //3.过滤出支付信息表数据
        Table paymentInfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "     `data`['id'] id,\n" +
                "     `data`['out_trade_no'] out_trade_no,\n" +
                "     `data`['order_id'] order_id,\n" +
                "     `data`['user_id'] user_id,\n" +
                "     `data`['payment_type'] payment_type,\n" +
                "     `data`['trade_no'] trade_no,\n" +
                "     `data`['total_amount'] total_amount,\n" +
                "     `data`['subject'] subject,\n" +
                "     `data`['payment_status'] payment_status,\n" +
                "     `data`['create_time'] create_time,\n" +
                "     `data`['callback_time'] callback_time,\n" +
                "     `data`['callback_content'] callback_content,\n" +
                "     `data`['operate_time'] operate_time,\n" +
                "     `rt`,\n"+
                "     `pt`\n"+
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='payment_info'\n" +
                "and `type`= 'insert' \n");
        tableEnv.createTemporaryView("payment_info",paymentInfoTable);
        //paymentInfoTable.execute().print();

        //tableEnv.toDataStream(paymentInfoTable).print("paymentInfoTable>>>>>>>>>");


        //3.读取kafka中dwd层的dwd_trade_order_detail数据并创建动态表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "    `order_id` string,\n" +
                "    `consignee` string,\n" +
                "    `consignee_tel` string,\n" +
                "    `total_amount` string,\n" +
                "    `order_status` string,\n" +
                "    `user_id` string,\n" +
                "    `payment_way` string,\n" +
                "    `delivery_address` string,\n" +
                "    `order_comment` string,\n" +
                "    `out_trade_no` string,\n" +
                "    `trade_body` string,\n" +
                "    `create_time` string,\n" +
                "    `operate_time` string,\n" +
                "    `expire_time` string,\n" +
                "    `process_status` string,\n" +
                "    `tracking_no` string,\n" +
                "    `parent_order_id` string,\n" +
                "    `province_id` string,\n" +
                "    `activity_reduce_amount` string,\n" +
                "    `coupon_reduce_amount` string,\n" +
                "    `original_total_amount` string,\n" +
                "    `feight_fee` string,\n" +
                "    `feight_fee_reduce` string,\n" +
                "    `refundable_time` string,\n" +
                "    `rt` TIMESTAMP_LTZ(3),\n" +

                "    `order_detail_id` string,\n" +
                "    `sku_id` string,\n" +
                "    `sku_name` string,\n" +
                "    `order_price` string,\n" +
                "    `sku_num` string,\n" +
                "    `source_type` string,\n" +
                "    `source_id` string,\n" +
                "    `split_total_amount` string,\n" +
                "    `split_activity_amount` string,\n" +
                "    `split_coupon_amount` string,\n" +

                "    `order_detail_activity_id` string,\n" +
                "    `activity_id` string,\n" +
                "    `activity_rule_id` string,\n" +

                "    `order_detail_coupon_id` string,\n" +
                "    `coupon_id` string,\n" +
                "    `coupon_use_id` string,\n" +
                "    WATERMARK FOR rt AS rt \n" +
                ")"+KafkaUtil.getKafkaSourceDDL("dwd_trade_order_detail","Dwd06_TradePayDetailSuc"));
        //tableEnv.sqlQuery("select * from dwd_trade_order_detail").execute().print();

//        Table table = tableEnv.sqlQuery("select * from dwd_trade_order_detail");
//        tableEnv.toDataStream(table)
//                .print("dwd_trade_order_detail>>>>>>>>>>");


        //4.读取hbase字典表
        tableEnv.executeSql(HBaseUtil.getBaseDicDDL());
//        tableEnv.sqlQuery("select * from dim_base_dic").execute().print();

        //5.interval join  dwd_trade_order_detail和payment_info
        Table resultTable = tableEnv.sqlQuery("" +
                        "select \n" +
                        "     od.order_id,\n" +
                        "     od.consignee,\n" +
                        "     od.consignee_tel,\n" +
                        "     od.total_amount,\n" +
                        "     od.order_status,\n" +
                        "     od.user_id,\n" +
                        "     od.payment_way,\n" +
                        "     od.delivery_address,\n" +
                        "     od.order_comment,\n" +
                        "     od.out_trade_no,\n" +
                        "     od.trade_body,\n" +
                        "     od.create_time,\n" +
                        "     od.operate_time,\n" +
                        "     od.expire_time,\n" +
                        "     od.process_status,\n" +
                        "     od.tracking_no,\n" +
                        "     od.parent_order_id,\n" +
                        "     od.province_id,\n" +
                        "     od.activity_reduce_amount,\n" +
                        "     od.coupon_reduce_amount,\n" +
                        "     od.original_total_amount,\n" +
                        "     od.feight_fee,\n" +
                        "     od.feight_fee_reduce,\n" +
                        "     od.refundable_time,\n" +
                        "     od.rt,\n" +
                        "     od.order_detail_id,\n" +
                        "     od.sku_id,\n" +
                        "     od.sku_name,\n" +
                        "     od.order_price,\n" +
                        "     od.sku_num,\n" +
                        "     od.source_type,\n" +
                        "     od.source_id,\n" +
                        "     od.split_total_amount,\n" +
                        "     od.split_activity_amount,\n" +
                        "     od.split_coupon_amount,\n" +
                        "     od.order_detail_activity_id,\n" +
                        "     od.activity_id,\n" +
                        "     od.activity_rule_id,\n" +
                        "     od.order_detail_coupon_id,\n" +
                        "     od.coupon_id,\n" +
                        "     od.coupon_use_id,\n" +
                        "     pi.id,\n" +
                        "     pi.payment_type,\n" +
                        "     dim_base_dic.info.dic_name payment_type_name,\n" +
                        "     pi.trade_no,\n" +
                        "     pi.subject,\n" +
                        "     pi.payment_status,\n" +
                        "     pi.callback_time,\n" +
                        "     pi.callback_content\n" +
                        "from  payment_info pi\n" +
                        "join  dwd_trade_order_detail od\n" +
                        "on pi.order_id = od.order_id\n" +
                        "and od.rt >= pi.rt - INTERVAL '15' MINUTE  \n" +
                        "and od.rt <= pi.rt + INTERVAL '60' SECOND\n" +
                        "join dim_base_dic FOR SYSTEM_TIME AS OF pi.pt \n" +
                        "on dim_base_dic.rowkey=pi.payment_type"
                );
        tableEnv.createTemporaryView("result_table",resultTable);
        //resultTable.execute().print();          //executeSql会自动阻塞,不关闭的话,后面代码不执行

        //构建kafka sink
        tableEnv.executeSql("" +
                "create table dwd_trade_pay_detail_suc (\n" +
                "       order_id string,\n" +
                "       consignee string,\n" +
                "       consignee_tel string,\n" +
                "       total_amount string,\n" +
                "       order_status string,\n" +
                "       user_id string,\n" +
                "       payment_way string,\n" +
                "       delivery_address string,\n" +
                "       order_comment string,\n" +
                "       out_trade_no string,\n" +
                "       trade_body string,\n" +
                "       create_time string,\n" +
                "       operate_time string,\n" +
                "       expire_time string,\n" +
                "       process_status string,\n" +
                "       tracking_no string,\n" +
                "       parent_order_id string,\n" +
                "       province_id string,\n" +
                "       activity_reduce_amount string,\n" +
                "       coupon_reduce_amount string,\n" +
                "       original_total_amount string,\n" +
                "       feight_fee string,\n" +
                "       feight_fee_reduce string,\n" +
                "       refundable_time string,\n" +
                "       rt TIMESTAMP_LTZ(3),\n" +
                "       order_detail_id string,\n" +
                "       sku_id string,\n" +
                "       sku_name string,\n" +
                "       order_price string,\n" +
                "       sku_num string,\n" +
                "       source_type string,\n" +
                "       source_id string,\n" +
                "       split_total_amount string,\n" +
                "       split_activity_amount string,\n" +
                "       split_coupon_amount string,\n" +
                "       order_detail_activity_id string,\n" +
                "       activity_id string,\n" +
                "       activity_rule_id string,\n" +
                "       order_detail_coupon_id string,\n" +
                "       coupon_id string,\n" +
                "       coupon_use_id string,\n" +
                "       id string,\n" +
                "       payment_type string,\n" +
                "       payment_type_name string,\n" +
                "       trade_no string,\n" +
                "       subject string,\n" +
                "       payment_status string,\n" +
                "       callback_time string,\n" +
                "       callback_content string,\n" +
                "      primary key(order_detail_id) not enforced\n" +
                ")"  +KafkaUtil.getUpsertKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));

        //写入到dwd_trade_pay_detail_suc
        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table");

        env.execute();
    }
}
