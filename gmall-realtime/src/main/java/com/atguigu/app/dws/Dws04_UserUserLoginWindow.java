package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.bean.UserLoginBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.DorisUtil;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/11/13-19:20
 *  用户域用户登陆各窗口汇总表
 *  主要任务:从 Kafka 页面日志主题读取数据，统计七日回流（回归）用户和当日独立用户数。
 *  粒度:无（无需分组,每条数据都要处理）
 *  度量值:独立用户数
 */
//数据流:web/app -> nginx -> 日志服务器(log文件) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris
//程  序:Mock -> Flume(f1.sh) -> Kafka(ZK) -> Dwd01_TrafficBaseLogSplit -> Kafka(ZK) -> Dws04_UserUserLoginWindow -> Doris(DWS)
public class Dws04_UserUserLoginWindow {
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

        //3.过滤数据并转为Json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String uId = jsonObject.getJSONObject("common").getString("uid");
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (uId != null && (lastPageId == null || "login".equals(lastPageId))) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据" + value);
                }
            }
        });

        //4.提取时间戳生成waterMake
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }
        ));

        //5.按照mid分组,去重,计算独立访客数,同时转换为JavaBean
        SingleOutputStreamOperator<UserLoginBean> userLoginDS = jsonObjWithWMDS.keyBy(json -> json.getJSONObject("common").getString("uid"))
                .flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
                    //状态里面存放用户上次登陆的日期,精确到天(yyyy-MM-dd)
                    ValueState<String> lastVisitDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-visit-dt", String.class);
                        lastVisitDtState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                        String lastDt = lastVisitDtState.value();
                        Long ts = value.getLong("ts");
                        String curDt = DateFormatUtil.toDate(ts);

                        Long uUv = 0L;
                        Long backUv = 0L;

                        //如果状态中没有登录信息(为今日首次登陆),则记为独立登录用户
                        if (lastDt == null) {
                            uUv = 1L;
                            lastVisitDtState.update(curDt);
                            //如果今天的登陆日期与上次不一样(为今日首次登陆),则记为独立登录用户
                        } else if (!curDt.equals(lastDt)) {
                            uUv = 1L;
                            lastVisitDtState.update(curDt);
                            //如果今天登陆的时间与上次不一样并且差值大于7天,记为回流用户
                            if ((ts - DateFormatUtil.toTs(lastDt, false)) / 24 * 3600 * 1000 >= 7) {
                                backUv = 1L;
                            }
                        }

                        //判断输出结果
                        if (uUv == 1L) {
                            out.collect(new UserLoginBean(
                                    "",
                                    "",
                                    curDt,
                                    backUv,
                                    uUv
                            ));
                        }
                    }
                });

        //6.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userLoginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(next);
                    }
                });
        resultDS.print();

        //7.写入Doris
        resultDS.map(bean->{
            SerializeConfig serializeConfig = new SerializeConfig();
            serializeConfig.propertyNamingStrategy= PropertyNamingStrategy.SnakeCase;
            return JSON.toJSONString(bean,serializeConfig);
        })
                .sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_user_user_login_window"));

        //8.执行任务
        env.execute();
    }
}
