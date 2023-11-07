package com.atguigu.common;

public class Constant {

    public static final String KAFKA_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String ZK_SERVERS = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    public static final String TOPIC_ODS_DB = "topic_db";
    public static final String TOPIC_ODS_LOG = "topic_log";

    public static final String HBASE_NAME_SPACE = "gmall_230524";

    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;

    //5个日志数据分区
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";

    //8个业务数据分区
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";
    public static final int ONE_DAY = 24 * 60 * 60;
}
