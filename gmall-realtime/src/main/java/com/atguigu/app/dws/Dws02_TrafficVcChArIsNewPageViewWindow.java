package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DorisUtil;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/11/11-9:19
 * 流量域版本-渠道-地区-访客类别 粒度页面浏览各窗口汇总表
 * 本节汇总表中需要有 页面浏览数、浏览总时长、会话数、独立访客数 四个度量字段。
 * 本节的任务是统计这四个指标，并将维度和度量数据写入Doris汇总表。
 *
 * 粒度:流量域版本-渠道-地区-访客类别
 * 度量值:页面浏览数、浏览总时长、会话数、独立访客数
 */
//数据流:web/app -> nginx -> 日志服务器(log文件) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris
//程  序:Mock -> Flume(f1.sh) -> Kafka(ZK) -> Dwd01_TrafficBaseLogSplit -> Kafka(ZK) -> Dws02_TrafficVcChArIsNewPageViewWindow -> Doris(DWS)
public class Dws02_TrafficVcChArIsNewPageViewWindow {
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

        //3.仅过滤脏数据并转为json对象
        SingleOutputStreamOperator<JSONObject> flatPageDS = pageDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (JSONException e) {
                    System.out.println("脏数据" + value);
                }
            }
        });

        //4.按照Mid进行分组
        KeyedStream<JSONObject, String> pageFlatByMidDS = flatPageDS.keyBy(key -> key.getJSONObject("common").getString("mid"));

        //5.去重Mid,并将数据封装为bean对象 (要用到状态,因此起步使用richMapFunction)
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewDS = pageFlatByMidDS.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            //状态:用来存储用户上次访问页面的日期(yyyy-MM-dd)
            private ValueState<String> valueState;
            //日期格式化器
            private SimpleDateFormat sdf;

            //open中将ttl,状态.日期格式化工具准备好
            @Override
            public void open(Configuration parameters) throws Exception {
                //设置状态ttl为一天过期
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//状态更新时机
                        .build();
                //获取状态描述器并开启ttl
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-visit-ts", String.class);
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                //状态初始化
                valueState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd"); //规定日期格式
            }

            @Override
            public TrafficPageViewBean map(JSONObject value) throws Exception {
                //拿到数据
                JSONObject common = value.getJSONObject("common");
                JSONObject page = value.getJSONObject("page");
                Long ts = value.getLong("ts");
                String curDt = sdf.format(ts);         //事件时间
                String lastDt = valueState.value();    //状态时间

                //独立访客数(独立访客（Unique visitor）是指通过互联网访问、浏览某个网页的自然人。)
                //(同一个用户第一次访问不同页面)
                Long uv = 0L;
                if (lastDt == null || !lastDt.equals(curDt)) {
                    uv = 1L;
                    valueState.update(curDt);
                }
                //会话数
                Long sv = 0L;
                if (page.getString("last_page_id") == null) {
                    sv = 1L;
                }
                //窗口信息后面补充
                return new TrafficPageViewBean(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        curDt,
                        uv,
                        sv,
                        1L,
                        page.getLong("during_time"),
                        ts
                );

            }
        });

        //6.提取事件时间提取waterMake(ts在TrafficPageViewBean中)
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithWMDS = trafficPageViewDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //7.分组开窗聚合(分组按粒度,聚合按度量)
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = trafficPageViewWithWMDS
                .keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                        return new Tuple4<>(
                                value.getIsNew(),
                                value.getAr(),
                                value.getCh(),
                                value.getVc()
                        );
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {      //4个度量值相加
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        return value1;
                    }
                }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        //获取数据
                        TrafficPageViewBean trafficPageViewBean = input.iterator().next();
                        trafficPageViewBean.setStt(sdf.format(window.getStart()));
                        trafficPageViewBean.setEdt(sdf.format(window.getEnd()));
                        //输出
                        out.collect(trafficPageViewBean);
                    }
                });
        resultDS.print();

        //8.写入doris
        resultDS.map(bean->{
            SerializeConfig serializeConfig = new SerializeConfig();
            // 转成json的时候, 属性名使用下划线
            serializeConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
            return JSON.toJSONString(bean,serializeConfig);
        }).sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_traffic_vc_ch_ar_is_new_page_view_window"));

        //9.启动任务
        env.execute("Dws02_TrafficVcChArIsNewPageViewWindow");

    }
}
