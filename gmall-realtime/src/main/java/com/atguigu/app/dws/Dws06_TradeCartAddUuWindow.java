package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.bean.CartAddUuBean;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/11/13-20:39
 * 交易域加购各窗口汇总表
 * 主要任务:从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 Doris。
 * 粒度:无（无需分组,每条数据都要处理）
 * 度量值:独立用户数
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Doris(DWS)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd03_TradeCartAdd -> Kafka(ZK) -> Dws06_TradeCartAddUuWindow -> Doris
public class Dws06_TradeCartAddUuWindow {
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

        //2.读取kafka wdw层中dwd_trade_cart_add表中的数据创建数据流
        DataStreamSource<String> kafkaDS = env.fromSource(KafkaUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_CART_ADD, "dws_cart_add"), WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.过滤脏数据,并转为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("脏数据" + value);
                }
            }
        });

        //4.提取时间戳生成waterMake
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
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
        SingleOutputStreamOperator<CartAddUuBean> cartAddUuDS = jsonObjWithWMDS
                .keyBy(json -> json.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //状态有效期为1天
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-cart-dt", String.class);
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                lastCartDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {

                String operateTime = value.getString("operate_time");
                String lastDt = lastCartDtState.value();
                String curDt;
                //拿到时间戳(格式为yyyy-MM-dd)
                if (operateTime != null) {
                    curDt = operateTime.split(" ")[0];
                } else {
                    curDt = value.getString("create_time").split(" ")[0];
                }

                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartDtState.update(curDt);
                    out.collect(new CartAddUuBean(
                            "",
                            "",
                            curDt,
                            1L));
                }
            }
        });

        //6.开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddUuDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        WindowFunctionUtil.setSttEdt(window, values, out);
                    }
                });
        resultDS.print();

        //7.将数据写入到Doris
        resultDS.map(bean->{
            SerializeConfig config = new SerializeConfig();
            config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;  // 转成json的时候, 属性名使用下划线
            return JSON.toJSONString(bean, config);
        }).sinkTo(DorisUtil.getDorisSink("gmall_dws.dws_trade_cart_add_uu_window"));

        //8.启动任务
        env.execute("Dws06_TradeCartAddUuWindow");

    }
}
