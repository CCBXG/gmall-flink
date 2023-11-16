package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.DorisUtil;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/11/11-16:34
 * 流量域首页、详情页页面浏览各窗口汇总表
 * 主要任务:从 Kafka页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。
 * 粒度:无（无需分组,每条数据都要处理）
 * 度量值:独立访客数
 */
//数据流:web/app -> nginx -> 日志服务器(log文件) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris
//程  序:Mock -> Flume(f1.sh) -> Kafka(ZK) -> Dwd01_TrafficBaseLogSplit -> Kafka(ZK) -> Dws03_TrafficHomeDetailPageViewWindow -> Doris(DWS)
public class Dws03_TrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境中,主题并行度与Kafka主题的分区数保持一致

        //Logger logger = LoggerFactory.getLogger(Dwd01_TrafficBaseLogSplit.class);
        //logger.info("aaa");

        //1.1 开启CheckPoint(写出到Doris时必须开启CheckPoint,时间设置多长,往外写的时候就多久写出一次)
        env.enableCheckpointing(5000L);
        env.setStateBackend(new HashMapStateBackend());

        //1.2 CheckPoint相关设置
        //CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //checkpointConfig.setCheckpointTimeout(10000L);
        //checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-ck");
        //Cancel任务时保存最后一次CheckPoint结果
        //checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        //设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));

        //2.读取kafka dwd层long中dwd_traffic_page表的主题数据
        //数据样式:{"common":{"ar":"31","uid":"253","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"OPPO Oneplus 10","mid":"mid_32","vc":"v2.1.132","ba":"OPPO","sid":"d15dcfe1-3056-4d11-84f0-2dbe5d50cf2c"},"page":{"page_id":"orders_all","during_time":10474,"last_page_id":"mine"},"ts":1699802895805}
        DataStreamSource<String> pageDS = env.fromSource(KafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRAFFIC_PAGE, "Vc_Ch_Ar_IsNew"), WatermarkStrategy.noWatermarks(),"kafka-source");

        //3.过滤并转换为json对象(仅保留page_id为 home 或 good_detail 的数据)
        SingleOutputStreamOperator<JSONObject> jsonObjDs = pageDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                        out.collect(jsonObject);
                    }
                } catch (JSONException e) {
                    System.out.println("脏数据:" + value);
                }
            }
        });

        //4.提取时间戳生成waterMake
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //5.按照Mid分组，计算独立访客数 同时转换为JavaBean对象
        KeyedStream<JSONObject, String> keyDS = jsonObjWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailPageViewDS = keyDS.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            //状态里面存放用户上次浏览首页或者商品浏览页的日期,精确到天(yyyy-MM-dd)
            private ValueState<String> homeListVisitDtState;
            private ValueState<String> detailListVisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //设置状态的过期时间ttl
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  //总状态保持一天
                        .build();
                //创建状态描述器
                ValueStateDescriptor<String> lastHomeDt = new ValueStateDescriptor<>("last-home-dt", String.class);
                ValueStateDescriptor<String> lastDetailDt = new ValueStateDescriptor<>("last-detail-dt", String.class);
                //创建状态
                homeListVisitDtState = getRuntimeContext().getState(lastHomeDt);
                detailListVisitDtState = getRuntimeContext().getState(lastDetailDt);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                //获取相关数据
                String pageId = value.getJSONObject("page").getString("page_id");
                //里面存储的日期为 yyyy-MM-dd(只到天)
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                //创建度量值计数器
                Long homeVc = 0L;
                Long goodDetailVc = 0L;
                //
                if ("home".equals(pageId)) {
                    String homeLastDt = homeListVisitDtState.value();
                    //同一个用户同一天第一次访问主页
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeVc = 1L;
                        homeListVisitDtState.update(curDt);
                    }
                } else {
                    String detailLastDt = detailListVisitDtState.value();
                    //同一个用户同一天第一次访问商品详情页
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        goodDetailVc = 1L;
                        detailListVisitDtState.update(curDt);
                    }
                }

                if (homeVc == 1L || goodDetailVc == 1L) {
                    out.collect(new TrafficHomeDetailPageViewBean(
                            "",
                            "",
                            curDt,
                            homeVc,
                            goodDetailVc
                    ));
                }
            }
        });

        //6.开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailPageViewDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(next);
                    }
                });
        resultDS.print();

        //7.将JavaBean数据转为json字符串写出(注意:JavaBean和Doris里面的字段名不一样,需要将小驼峰转为蛇形)
        resultDS
                .map(bean->{
            SerializeConfig serializeConfig = new SerializeConfig();
            serializeConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
            return JSON.toJSONString(bean,serializeConfig);
        })
                .sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_traffic_home_detail_page_view_window"));

        //8.启动任务
        env.execute("Dws03_TrafficHomeDetailPageViewWindow");

    }
}
