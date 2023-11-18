package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.app.func.DimInfoRichMapFunction;
import com.atguigu.app.func.DimInfoRichMapFunctionAsync;
import com.atguigu.bean.TradeSkuOrderBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DorisUtil;
import com.atguigu.util.KafkaUtil;
import com.atguigu.util.WindowFunctionUtil;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author 城北徐公
 * @Date 2023/11/14-18:28
 * 交易域商品粒度下单各窗口汇总表
 * 主要任务:从Kafka订单明细主题读取数据，过滤null数据并按照唯一键对数据去重，按照SKU维度分组，
 * 统计原始金额、活动减免金额、优惠券减免金额和订单金额，并关联维度信息，将数据写入 Doris 交易域SKU粒度下单各窗口汇总表。
 * 粒度:order_detail_id
 * 度量值:原始金额、活动减免金额、优惠券减免金额和订单金额
 * 使用了旁路缓存技术+异步
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris(DWS)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd04_TradeOrderDetail -> Kafka(ZK) -> Dws09_TradeSkuOrderWindow(HBase HDFS ZK Redis) -> Doris
public class Dws09_TradeSkuOrderWindowAsync {
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

        //3.过滤null值,转化为json对象,取waterMake
        SingleOutputStreamOperator<JSONObject> jsonObjWMDS = kafkaDS
                .flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(value);
                        out.collect(jsonObject);
                    } catch (Exception e) {
                        System.out.println("脏数据:" + value);
                    }
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("create_time");
            }
        }));

        //4.去重（相同的order_detail_id,保留第一条数据就行） 转化为JavaBean对象
        SingleOutputStreamOperator<TradeSkuOrderBean> flatDS = jsonObjWMDS
                .keyBy(json -> json.getString("order_detail_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradeSkuOrderBean>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //设置ttl为5秒(状态存储5秒)
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeSkuOrderBean> out) throws Exception {
                        String state = valueState.value();
                        if (state == null) {
                            valueState.update("1");
                            BigDecimal splitActivityAmount = value.getBigDecimal("split_activity_amount");
                            BigDecimal splitCouponAmount = value.getBigDecimal("split_coupon_amount");

                            out.collect(
                                    TradeSkuOrderBean.builder()
                                            .skuId(value.getString("sku_id"))
                                            .skuName(value.getString("sku_name"))
                                            .originalAmount(value.getBigDecimal("original_total_amount"))
                                            .orderAmount(value.getBigDecimal("split_total_amount"))
                                            .curDate(value.getString("create_time").split(" ")[0])
                                            .activityAmount(splitActivityAmount == null ? new BigDecimal("0.0") : splitActivityAmount)
                                            .couponAmount(splitCouponAmount == null ? new BigDecimal("0.0") : splitCouponAmount)
                                            .build());
                        }
                    }
                });


        //5.分组(sku)开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = flatDS.keyBy(TradeSkuOrderBean::getSkuId)//粒度分组
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        //度量值聚合:原始金额、活动减免金额、优惠券减免金额和订单金额
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                        value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                        return value1;
                    }
                }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        WindowFunctionUtil.setSttEdt(window, input, out);
                    }
                });

        //6.关联维表补充维度信息(sku -> sku spu tm c1 c2 c3)  优化
        //关联dim_sku_info表,补充spu_id,tm_id,category3_id
        SingleOutputStreamOperator<TradeSkuOrderBean> skuDS = AsyncDataStream.unorderedWait(reduceDS, new DimInfoRichMapFunctionAsync<TradeSkuOrderBean>("dim_sku_info") {
            @Override
            protected String getPk(TradeSkuOrderBean input) {
                return input.getSkuId();
            }

            @Override
            protected void join(TradeSkuOrderBean input, JSONObject dimInfoJson) {
                input.setSpuId(dimInfoJson.getString("spu_id"));
                input.setTrademarkId(dimInfoJson.getString("tm_id"));
                input.setCategory3Id(dimInfoJson.getString("category3_id"));
            }
        }, 100, TimeUnit.SECONDS);

        //关联dim_spu_info,补充spu_name
        SingleOutputStreamOperator<TradeSkuOrderBean> spuDS = AsyncDataStream.unorderedWait(skuDS, new DimInfoRichMapFunctionAsync<TradeSkuOrderBean>("dim_spu_info") {
            @Override
            protected String getPk(TradeSkuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            protected void join(TradeSkuOrderBean input, JSONObject dimInfoJson) {
                input.setSpuName(dimInfoJson.getString("spu_name"));
            }
        }, 100, TimeUnit.SECONDS);

        //关联dim_base_trademark,补充tm_name
        SingleOutputStreamOperator<TradeSkuOrderBean> tmDS = AsyncDataStream.unorderedWait(spuDS, new DimInfoRichMapFunctionAsync<TradeSkuOrderBean>("dim_base_trademark") {
            @Override
            protected String getPk(TradeSkuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            protected void join(TradeSkuOrderBean input, JSONObject dimInfoJson) {
                input.setTrademarkName(dimInfoJson.getString("tm_name"));
            }
        }, 100, TimeUnit.SECONDS);

        //关联dim_base_category3,补充name(category3的name),category2_id
        SingleOutputStreamOperator<TradeSkuOrderBean> c3DS = AsyncDataStream.unorderedWait(tmDS, new DimInfoRichMapFunctionAsync<TradeSkuOrderBean>("dim_base_category3") {
            @Override
            protected String getPk(TradeSkuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            protected void join(TradeSkuOrderBean input, JSONObject dimInfoJson) {
                input.setCategory3Name(dimInfoJson.getString("name"));
                input.setCategory2Id(dimInfoJson.getString("category2_id"));
            }
        }, 100, TimeUnit.SECONDS);

        //关联dim_base_category2,补充name(category2的name),category2_id
        SingleOutputStreamOperator<TradeSkuOrderBean> c2DS = AsyncDataStream.unorderedWait(c3DS, new DimInfoRichMapFunctionAsync<TradeSkuOrderBean>("dim_base_category2") {
            @Override
            protected String getPk(TradeSkuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            protected void join(TradeSkuOrderBean input, JSONObject dimInfoJson) {
                input.setCategory2Name(dimInfoJson.getString("name"));
                input.setCategory1Id(dimInfoJson.getString("category1_id"));
            }
        }, 100, TimeUnit.SECONDS);

        //关联dim_base_category1,补充name(category1的name)
        SingleOutputStreamOperator<TradeSkuOrderBean> c1DS = AsyncDataStream.unorderedWait(c2DS, new DimInfoRichMapFunctionAsync<TradeSkuOrderBean>("dim_base_category1") {
            @Override
            protected String getPk(TradeSkuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            protected void join(TradeSkuOrderBean input, JSONObject dimInfoJson) {
                input.setCategory1Name(dimInfoJson.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);
        c1DS.print();

        //7.写出
        c1DS.map(bean -> {
            SerializeConfig serializeConfig = new SerializeConfig();
            serializeConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
            return JSON.toJSONString(bean,serializeConfig);
        }).sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_trade_sku_order_window"));

        //8.启动
        env.execute();
    }
}
