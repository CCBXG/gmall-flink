package com.atguigu.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DwdTableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @Author 城北徐公
 * @Date 2023/11/8-11:13
 * 事实表动态分流
 * 任务:DWD层余下的事实表都是从topic_db中取业务数据库一张表的变更数据，按照某些条件过滤后写入Kafka的对应主题，
 * 它们处理逻辑相似且较为简单，可以结合配置表动态分流在同一个程序中处理。(类似于dim动态分流)
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> Dwd09_BaseDbApp -> Kafka(ZK)
public class Dwd09_BaseDbApp {
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

        //2.读取kafka topic_db主题数据创建数据流(为了方便复用,将kafkaSource的创建抽取到工具类中)
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Constant.TOPIC_ODS_DB, "base_db_app");
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.将数据流每行数据转换为JSON对象 过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (value != null) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(value);
                        out.collect(jsonObject);
                    } catch (JSONException e) {
                        System.out.println("脏数据:" + value);
                    }
                }
            }
        });

        //4.使用flinkCdc读取配置信息表 并做成广播
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username("root")
                .password("000000")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mysqlDS.broadcast(mapStateDescriptor);

        //5.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjDS.connect(broadcast);

        //6.处理连接流,根据广播信息过滤主流数据
        SingleOutputStreamOperator<JSONObject> resultDS = connectDS.process(new DwdTableProcessFunction(mapStateDescriptor));

        //7.将数据写出到kafka(一次要写入到多个主题,不能使用原来的只传一个主题的sink)
        //resultDS.sinkTo(KafkaUtil.getKafkaSink());
        resultDS.sinkTo(KafkaUtil.getKafkaSink(new KafkaRecordSerializationSchema<JSONObject>() {
            @Nullable
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                return new ProducerRecord<>(
                        element.getString("sink_topic"),
                        element.getString("data").getBytes()
                );
            }
        }));

        //8.启动任务
        env.execute();

    }
}
