package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimCreateTableMapFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.DimTableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Constant;
import com.atguigu.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/11/3-18:48
 * hbase中dim维表写入
 */
//数据流：web/app -> Nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> HBase(DIM)
//程  序：Mock -> maxwell.sh -> Kafka(ZK) -> DimApp -> HBase(HDFS ZK)
public class DimApp {
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
        //主流数据样式:{"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null}}
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Constant.TOPIC_ODS_DB, "dim-app");
        DataStreamSource<String> kafkaDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

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

        //4.使用FlinkCDC读取配置信息表数据
        //广播流数据样式:FlinkCDC：{"op":"d","before":{"sink_row_key":"id","sink_type":"dim","sink_family":"info","source_type":"ALL","sink_table":"dim_user_info","source_table":"user_info","sink_columns":"id,login_name,name,user_level,birthday,gender,create_time,operate_time"},"source":{"thread":24,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000081","connector":"mysql","pos":10545,"name":"mysql_binlog_source","row":0,"ts_ms":1699430248000,"snapshot":"false","db":"gmall_config","table":"table_process"},"ts_ms":1699430248100}
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

        //5.将配置信息流做成广播流并与数据流进行连接
        //   key使用的是sourceTable字段，value使用的是TableProcess的JavaBean
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("bc-state", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcast = mysqlDS
                .map(new DimCreateTableMapFunction())
                .broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcess> connectDS = jsonObjDS.connect(broadcast);

        //6.处理连接流  根据配置信息过滤数据流
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectDS.process(new DimTableProcessFunction(mapStateDescriptor));
        hbaseDS.print();

        //7.将过滤后的数据写入到hbase中
        DataStreamSink<JSONObject> sinkDS = hbaseDS.addSink(new DimSinkFunction());

        //8.启动任务
        env.execute("DimApp");
    }
}
