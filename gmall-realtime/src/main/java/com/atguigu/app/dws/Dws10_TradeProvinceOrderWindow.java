package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.app.func.DimInfoRichMapFunctionAsync;
import com.atguigu.bean.TradeProvinceOrderBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;

import com.atguigu.util.DorisUtil;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author 城北徐公
 * @Date 2023/11/17-18:15
 * 交易域省份粒度下单各窗口汇总表
 * 主要任务:从 Kafka 读取订单明细数据，过滤 null 数据并按照唯一键对数据去重，
 * 统计各省份各窗口订单数和订单金额，将数据写入Doris 交易域省份粒度下单各窗口汇总表。
 * 粒度:省份
 * 度量值:订单数,订单金额
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris(DWS)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd04_TradeOrderDetail -> Kafka(ZK) -> Dws10_TradeProvinceOrderWindow(HBase HDFS ZK Redis) -> Doris
public class Dws10_TradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境中,主题并行度与Kafka主题的分区数保持一致

        //Logger logger = LoggerFactory.getLogger(Dwd01_TrafficBaseLogSplit.class);
        //logger.info("aaa");

        //1.1 开启CheckPoint
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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000L));

        //2.读取kafka dwd层的 dwd_trade_order_detail 下单主题数据
        DataStreamSource<String> kafkaDS = env.fromSource(KafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "sku_order"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.过滤null值,转为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        out.collect(jsonObject);
                    } catch (Exception e) {
                        System.out.println("脏数据：" + value);
                    }
                }
            }
        });

        //4.提取时间戳生成waterMake
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("create_time");
            }
        }));

        //5.按照order_detail_id分组去重由left join产生的数据 同时转换为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderBean> tradeProvinceOrderDS = jsonObjWithWMDS
                .keyBy(json -> json.getString("order_detail_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradeProvinceOrderBean>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StateTtlConfig ttlConfig = new StateTtlConfig
                                .Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeProvinceOrderBean> out) throws Exception {
                        String state = valueState.value();
                        if (state == null) {
                            valueState.update("1");
                            HashSet<String> orderIds = new HashSet<>();
                            orderIds.add(value.getString("order_id"));
                            out.collect(new TradeProvinceOrderBean(
                                    "",
                                    "",
                                    value.getString("province_id"),
                                    "",
                                    orderIds,
                                    value.getString("create_time").split(" ")[0],
                                    0L,
                                    value.getBigDecimal("split_total_amount")
                            ));
                        }
                    }
                });

        // 6.分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = tradeProvinceOrderDS
                .keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.hours(3)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIds().addAll(value2.getOrderIds());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean provinceOrderBean = input.iterator().next();
                        provinceOrderBean.setOrderCount((long) provinceOrderBean.getOrderIds().size());
                        provinceOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        provinceOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(provinceOrderBean);
                    }
                });

        //7.补充维度信息
        SingleOutputStreamOperator<TradeProvinceOrderBean> resultDS = AsyncDataStream.unorderedWait(reduceDS, new DimInfoRichMapFunctionAsync<TradeProvinceOrderBean>("dim_base_province") {
            @Override
            protected String getPk(TradeProvinceOrderBean input) {
                return input.getProvinceId();
            }

            @Override
            protected void join(TradeProvinceOrderBean input, JSONObject dimInfoJson) {
                input.setProvinceName(dimInfoJson.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);
        resultDS.print();

        //7.写出到Doris
        resultDS.map(bean->{
            SerializeConfig serializeConfig = new SerializeConfig();
            serializeConfig.propertyNamingStrategy= PropertyNamingStrategy.SnakeCase;
            return JSON.toJSONString(bean,serializeConfig);
        }).sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_trade_province_order_window"));
        //8.启动任务
        env.execute();

    }
}
