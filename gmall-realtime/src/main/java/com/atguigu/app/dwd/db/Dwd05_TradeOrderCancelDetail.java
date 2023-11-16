package com.atguigu.app.dwd.db;

import com.atguigu.common.Constant;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/11/8-8:29
 * 交易域取消订单事务事实表
 * 从 Kafka 读取topic_db主题数据，关联筛选订单明细表、取消订单数据、
 * 订单明细活动关联表、订单明细优惠券关联表四张事实业务表形成取消订单明细表，写入 Kafka 对应主题。
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd04_TradeOrderDetail -> Kafka(ZK)
public class Dwd05_TradeOrderCancelDetail {
    public static void main(String[] args) {
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

        //3.过滤出订单表
        Table orderinfoTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['consignee'] consignee,\n" +
                "    `data`['consignee_tel'] consignee_tel,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['order_status'] order_status,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_way'] payment_way,\n" +
                "    `data`['delivery_address'] delivery_address,\n" +
                "    `data`['order_comment'] order_comment,\n" +
                "    `data`['out_trade_no'] out_trade_no,\n" +
                "    `data`['trade_body'] trade_body,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['expire_time'] expire_time,\n" +
                "    `data`['process_status'] process_status,\n" +
                "    `data`['tracking_no'] tracking_no,\n" +
                "    `data`['parent_order_id'] parent_order_id,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `data`['activity_reduce_amount'] activity_reduce_amount,\n" +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount,\n" +
                "    `data`['original_total_amount'] original_total_amount,\n" +
                "    `data`['feight_fee'] feight_fee,\n" +
                "    `data`['feight_fee_reduce'] feight_fee_reduce,\n" +
                "    `data`['refundable_time'] refundable_time,\n" +
                "     `pt` pt\n"+
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`= 'update' \n" +
                "and `old`['order_status'] is not null\n" +
                "and `data`['order_status'] = '1003'");
        tableEnv.createTemporaryView("order_info",orderinfoTable);

        //4.过滤出订单明细表
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select\n" +
                "   `data`['id'] id,\n" +
                "   `data`['order_id'] order_id,\n" +
                "   `data`['sku_id'] sku_id,\n" +
                "   `data`['sku_name'] sku_name,\n" +
                "   `data`['order_price'] order_price,\n" +
                "   `data`['sku_num'] sku_num,\n" +
                "   `data`['create_time'] create_time,\n" +
                "   `data`['source_type'] source_type,\n" +
                "   `data`['source_id'] source_id,\n" +
                "   `data`['split_total_amount'] split_total_amount,\n" +
                "   `data`['split_activity_amount'] split_activity_amount,\n" +
                "   `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "   `data`['operate_time'] operate_time\n" +
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type`= 'insert'");
        tableEnv.createTemporaryView("order_detail",orderDetailTable);

        //5.过滤出订单明细活动表
        Table orderDetailActivity = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time\n" +
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`= 'insert' ");
        tableEnv.createTemporaryView("order_detail_activity",orderDetailActivity);

        //6.过滤出订单明细优惠券表
        Table orderDetailCoupon = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['coupon_id'] coupon_id,\n" +
                "    `data`['coupon_use_id'] coupon_use_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time\n" +
                "from ods_topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`= 'insert' ");
        tableEnv.createTemporaryView("order_detail_coupon",orderDetailCoupon);

        //7.  4表join
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "\n" +
                "oi.id order_id,\n" +
                "oi.consignee,\n" +
                "oi.consignee_tel,\n" +
                "oi.total_amount,\n" +
                "oi.order_status,\n" +
                "oi.user_id,\n" +
                "oi.payment_way,\n" +
                "oi.delivery_address,\n" +
                "oi.order_comment,\n" +
                "oi.out_trade_no,\n" +
                "oi.trade_body,\n" +
                "oi.create_time,\n" +
                "oi.operate_time,\n" +
                "oi.expire_time,\n" +
                "oi.process_status,\n" +
                "oi.tracking_no,\n" +
                "oi.parent_order_id,\n" +
                "oi.province_id,\n" +
                "oi.activity_reduce_amount,\n" +
                "oi.coupon_reduce_amount,\n" +
                "oi.original_total_amount,\n" +
                "oi.feight_fee,\n" +
                "oi.feight_fee_reduce,\n" +
                "oi.refundable_time,\n" +
                "oi.pt,\n" +

                "od.id order_detail_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.order_price,\n" +
                "od.sku_num,\n" +
                "od.source_type,\n" +
                "od.source_id,\n" +
                "od.split_total_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +

                "oa.id order_detail_activity_id,\n" +
                "oa.activity_id,\n" +
                "oa.activity_rule_id,\n" +

                "oc.id order_detail_coupon_id,\n" +
                "oc.coupon_id,\n" +
                "oc.coupon_use_id\n" +

                "from order_info oi\n" +
                "join order_detail od\n" +
                "on oi.id=od.order_id\n" +
                "left join order_detail_activity oa\n" +
                "on od.id=oa.order_detail_id\n" +
                "left join order_detail_coupon oc\n" +
                "on od.id=oc.order_detail_id");
        tableEnv.createTemporaryView("result_table",resultTable);

        //8.构建kafka sink table
        tableEnv.executeSql("" +
                "create table dwd_order_cancel(\n" +
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
                "    `pt` TIMESTAMP_LTZ(3),\n" +

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
                "     PRIMARY KEY (`order_detail_id`) NOT ENFORCED\n" +
                ")"+KafkaUtil.getUpsertKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CANCEL_DETAIL));

        //9. 写出到kafka
        tableEnv.executeSql("insert into dwd_order_cancel select * from result_table");

    }
}
