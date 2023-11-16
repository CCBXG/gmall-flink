package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.bean.TradeOrderBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.DorisUtil;
import com.atguigu.util.KafkaUtil;
import com.atguigu.util.WindowFunctionUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
 * @Date 2023/11/14-8:35
 * 交易域下单各窗口汇总表(练习)
 * 主要任务:从 Kafka订单明细主题读取数据，统计当日下单独立用户数和首次下单用户数，封装为实体类，写入Doris。
 * 粒度:
 * 度量值:1
 */
public class Dws08_TradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000L));

        //2.读取kafka dwd层中的dwd_trade_order_detail表中的数据创建数据流
        DataStreamSource<String> kafkaDS = env.fromSource(KafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "dws_order_add"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.过滤脏数据,转为json格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                //因为dwd_trade_order_detail是一个撤回流，因此会有为null的数据,要先去除掉
                if (value != null) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(value);
                        out.collect(jsonObject);
                    } catch (Exception e) {
                        System.out.println("脏数据:" + value);
                    }
                }
            }
        });

        //4.提取时间戳,生成waterMake
        SingleOutputStreamOperator<JSONObject> jsonObjDSWM = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                Long operateTime = element.getLong("operate_time");
                if (operateTime != null) {
                    return operateTime;
                } else {
                    return element.getLong("create_time");
                }
            }
        }));

        //5.按照user_id分组进行去重,同时转化为JavaBean对象
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = jsonObjDSWM.keyBy(json -> json.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order-dt", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                        String lastDt = lastOrderDtState.value();
                        String curDt = value.getString("create_time").split(" ")[0];

                        Long curUv = 0L;
                        Long newUv = 0L;

                        //首次下单(首次下单包括当日下单)
                        if (lastDt == null) {
                            curUv = 1L;
                            newUv = 1L;
                            lastOrderDtState.update(curDt);
                            //当日下单
                        } else if (!lastDt.equals(curDt)) {
                            curUv = 1L;
                            lastOrderDtState.update(curDt);
                        }

                        if (curUv == 1L) {
                            out.collect(new TradeOrderBean(
                                    "",
                                    "",
                                    curDt,
                                    curUv,
                                    newUv
                            ));
                        }
                    }
                });

        //6.开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDS = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        WindowFunctionUtil.setSttEdt(window, values, out);
                    }
                });
        resultDS.print();

        //7.写出数据到Doris
        resultDS.map(bean->{
            SerializeConfig serializeConfig = new SerializeConfig();
            serializeConfig.propertyNamingStrategy= PropertyNamingStrategy.SnakeCase;
            return JSON.toJSONString(bean,serializeConfig);
        })
                .sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_trade_order_window"));

        //8.启动任务
        env.execute("Dws08_TradeOrderWindow");
    }
}
