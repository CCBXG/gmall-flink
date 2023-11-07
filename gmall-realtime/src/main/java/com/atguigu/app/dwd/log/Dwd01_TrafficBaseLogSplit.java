package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import com.atguigu.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @Author 城北徐公
 * @Date 2023/11/6-19:49
 * 流量域未经加工的事务事实表（日志分流）
 * 任务:数据清洗,新老访客状态标记修复,分流
 */
//数据流:web/app -> nginx -> 日志服务器(log文件) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序:Mock -> Flume(f1.sh) -> Kafka(ZK) -> Dwd01_TrafficBaseLogSplit -> Kafka(ZK)
public class Dwd01_TrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        //2.读取Kafka topic_log 主题数据创建流
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Constant.TOPIC_ODS_LOG, "Dwd01_TrafficBaseLogSplit");
        DataStreamSource<String> kfDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.过滤脏数据并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kfDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("脏数据" + value);
                }
            }
        });

        //4.按照mid进行分组,新老访客标记修复   状态编程    Rich
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> firstVisitState; //状态里面放的是用户第一次访客时间
                    private SimpleDateFormat sdf;  //时间戳格式化器

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first-visit-dt", String.class));
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    /**
                     *标记 1
                     *      状态    无    新                  状态改为今天
                     *      状态   今天   新                  状态改为今天
                     *      状态 ！今天   老用户清过缓存        标记 1改0
                     * 标记 0
                     *      状态   无     实时来之前老用户      状态 改1970
                     *      状态 ！今天   老用户               不改
                     * @param value The input value.
                     * @return 修复过的新老用户标记的value
                     * @throws Exception
                     */
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNow = value.getJSONObject("common").getString("is_new");
                        String firstDt = firstVisitState.value();
                        Long ts = value.getLong("ts");
                        String curDt = sdf.format(ts);
                        if ("1".equals(isNow)) {
                            if (!firstDt.equals(curDt)) {
                                value.getJSONObject("common").put("is_now", "0");
                            } else {
                                firstVisitState.update(curDt);
                            }
                        } else {
                            if (firstDt == null) {
                                firstVisitState.update("1970-01-01");
                            }
                        }
                        return value;
                    }
                });

        //5.分流  将页面数据写入主流  启动、曝光、动作、错误写入侧流   process
        //  错误
        //  启动
        //  页面:(启动后为页面page)
        //      曝光
        //      动作
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                //尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errorTag, value.toJSONString());
                }

                //尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                } else { //页面common可能包含 曝光 和 动作

                    JSONObject common = value.getJSONObject("common");
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        //将曝光数据遍历输出
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page_id", pageId);
                            ctx.output(displayTag, display.toJSONString());
                        }
                        //移除曝光数据
                        value.remove("displays");
                    }

                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        //将动作数据遍历输出
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page_id", pageId);
                            ctx.output(actionTag, action.toJSONString());
                        }
                        //移除动作数据
                        value.remove("actions");
                    }

                    //将页面日志写出到主流
                    out.collect(value.toJSONString());
                }
            }
        });

        //6.提取侧流数据,并写出到Kafka
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        SideOutputDataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        pageDS.print("pageDS>>>");
        startDS.print("startDS>>>");
        displayDS.print("displayDS>>>");
        actionDS.print("actionDS>>>");
        errorDS.print("errorDS>>>");

        pageDS.sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startDS.sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        errorDS.sinkTo(KafkaUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        //7.启动任务
        env.execute("Dwd01_TrafficBaseLogSplit");


    }
}
