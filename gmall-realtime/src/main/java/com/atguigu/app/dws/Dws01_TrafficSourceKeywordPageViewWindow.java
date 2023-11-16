package com.atguigu.app.dws;

import com.atguigu.app.func.DwsSplitFunction;
import com.atguigu.common.Constant;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/11/10-14:02
 *  流量域搜索关键词粒度页面浏览各窗口汇总表
 *  主要任务:从 Kafka 页面浏览明细主题读取数据，过滤搜索行为，使用自定义 UDTF（一进多出）函数对搜索内容分词。
 *  统计各窗口各关键词出现频次，写入 Doris。
 *  粒度:无（所有数据都要处理）
 *  度量值:1
 */
//数据流:web/app -> nginx -> 日志服务器(log文件) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris
//程  序:Mock -> Flume(f1.sh) -> Kafka(ZK) -> Dwd01_TrafficBaseLogSplit -> Kafka(ZK) -> Dws01_TrafficSourceKeywordPageViewWindow -> Doris(DWS)
public class Dws01_TrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境中,主题并行度与Kafka主题的分区数保持一致
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //Logger logger = LoggerFactory.getLogger(Dwd01_TrafficBaseLogSplit.class);
        //logger.info("aaa");

        //1.1 开启CheckPoint(写出到Doris时必须开启CheckPoint,时间设置多长,往外写的时候就多久写出一次)
        env.enableCheckpointing(5000L);
        env.setStateBackend(new HashMapStateBackend());

        //1.2 CheckPoint相关设置
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointTimeout(10000L);
//        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-ck");
//        Cancel任务时保存最后一次CheckPoint结果
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
//        设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));

        // 2.使用FlinkSQL读取Kafka 页面日志主题数据创建表
        //数据样式:{"common":{"ar":"33","ba":"xiaomi","ch":"xiaomi","is_new":"1","md":"xiaomi 12 ultra ","mid":"mid_269","os":"Android 12.0","sid":"564eedf4-4417-4d76-8065-e47565d072e2","uid":"426","vc":"v2.1.134"},"displays":[{"item":"19","item_type":"sku_id","pos_id":10,"pos_seq":0},{"item":"8","item_type":"sku_id","pos_id":10,"pos_seq":1},{"item":"6","item_type":"sku_id","pos_id":10,"pos_seq":2},{"item":"30","item_type":"sku_id","pos_id":10,"pos_seq":3},{"item":"25","item_type":"sku_id","pos_id":10,"pos_seq":4},{"item":"10","item_type":"sku_id","pos_id":10,"pos_seq":5},{"item":"18","item_type":"sku_id","pos_id":10,"pos_seq":6},{"item":"13","item_type":"sku_id","pos_id":10,"pos_seq":7},{"item":"3","item_type":"sku_id","pos_id":10,"pos_seq":8}],"page":{"during_time":7068,"item":"尚硅谷多线程教学课程","item_type":"keyword","last_page_id":"search","page_id":"good_list"},"ts":1654701675413}
        tableEnv.executeSql("" +
                "create table log_page(\n" +
                "    `page` map<string,string>,\n" +
                "    `ts` bigint,\n" +
                "    `rt` as TO_TIMESTAMP_LTZ(`ts`,3),\n" +
                "    WATERMARK FOR `rt` AS `rt` - INTERVAL '2' SECOND\n" +
                ")"+ KafkaUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"keyword_page_view_230524"));

        //tableEnv.sqlQuery("select * from log_page").execute().print();
        // 3.过滤出所需要的搜索数据
        Table pendingKeywordTable = tableEnv.sqlQuery("" +
                "select \n" +
                " `page`['item'] item,\n" +
                " `rt`\n" +
                "from log_page\n" +
                "where  `page`['last_page_id'] = 'search'\n" +
                "and    `page`['item'] is not null\n" +
                "and    `page`['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("pending_key_word",pendingKeywordTable);
        //pendingKeywordTable.execute().print();

        // 4.注册UDTF,并使用分词工具
        tableEnv.createTemporaryFunction("split_fun", DwsSplitFunction.class);
        Table keywordsTable = tableEnv.sqlQuery("" +
                "select\n" +
                " word,\n" +
                " rt\n" +
                "from pending_key_word, LATERAL TABLE(split_fun(item))");
        tableEnv.createTemporaryView("key_words",keywordsTable);

        // 5.(按照每一个拆分过的关键词分组) 分组开窗聚合 (按照*聚合)
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "      DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt,    --window_start,window_end自带的窗口信息\n" +
                "      DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "      word keyword,\n" +
                "      DATE_FORMAT(window_start,'yyyy-MM-dd') cur_date,   --分区键\n" +
                "      count(*)  keyword_count\n" +
                "from TABLE(\n" +
                "   TUMBLE(\n" +
                "     DATA => TABLE key_words,            --表名\n" +
                "     TIMECOL => DESCRIPTOR(rt),          --用什么当作分组时间 \n" +
                "     SIZE => INTERVAL '10' MINUTES))    --窗口大小,最小为10s\n" +
                "group by window_start, window_end,word\n");
        //resultTable.execute().print();

        // 6.数据输出到Doris
        tableEnv.executeSql("CREATE table doris_t(  " +
                " stt string, " +
                " edt string, " +
                " keyword string, " +
                " cur_date string, " +
                " keyword_count bigint " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                " 'table.identifier' = 'gmall_dws.dws_traffic_source_keyword_page_view_window', " +
                "  'username' = 'root', " +
                "  'password' = '000000', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.properties.read_json_by_line' = 'true', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ")");
        resultTable.insertInto("doris_t")
                .execute()
                .print();

        // 7.启动
        env.execute("Dws01_TrafficSourceKeywordPageViewWindow");

    }
}
