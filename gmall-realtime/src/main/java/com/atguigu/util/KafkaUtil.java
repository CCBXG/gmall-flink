package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @Author 城北徐公
 * @Date 2023/11/3-19:34
 * 获取kafkaSource
 */
public class KafkaUtil {
    /**
     * kafkaSource配置
     *
     * @param topic   要读的kafka主题
     * @param groupId 消费者组id
     * @return
     */
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {  //自定义序列化器，用于过滤掉空数据
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message == null) {
                            return null;
                        } else {
                            return new String(message);
                        }
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false; //判断是否是最后一条数据（实时没有最后一条数据）
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO; //不知道怎么写，从源码中拿
                    }
                })
                .build();
    }

    /**
     * kafkaSink配置
     * @param topic
     * @return
     */
    public static KafkaSink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_SERVERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
//                .setTransactionalIdPrefix("pre-")
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }

    //sink写入多个主题(缺点:主题字段key固定为sink_topic, 待写入字段的key固定为data )
    public static KafkaSink<JSONObject>  getKafkaSink(){
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_SERVERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                        return new ProducerRecord<>(element.getString("sink_topic"),
                                element.getString("data").getBytes());
                    }
                })
                .build();
    }

    //通用的kafkaSink
    public static <T>KafkaSink<T>  getKafkaSink(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema){
        return KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_SERVERS)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .build();
    }





    /**
     * 获取kafka中所有topic_db中的数据
     *
     * @param groupId
     * @return
     */
    public static String getTopicDbDDL(String groupId) {
        return "create table ods_topic_db(\n" +
                "    `database` string,\n" +
                "    `table` string,\n" +
                "    `type` string,\n" +
                "    `ts` bigint,\n" +
                "    `data` map<string,string>,\n" +
                "    `old` map<string,string>,\n" +
                "    `pt` AS PROCTIME(),\n" +
                "    `rt` as TO_TIMESTAMP_LTZ(`ts`,0),\n" +
                "    WATERMARK FOR `rt` AS `rt` - INTERVAL '2' SECOND\n" +
                ")" + getKafkaSourceDDL("topic_db", groupId);
    }

    /**
     * flinkSql kafkaSource
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static String getKafkaSourceDDL(String topic, String groupId) {
        return "with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '" + topic + "',\n" +
                "    'properties.bootstrap.servers' = '" + Constant.KAFKA_SERVERS + "',\n" +
                "    'properties.group.id' = '" + groupId + "',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";
    }

    /**
     * kafkaSink
     *
     * @param topic
     * @return
     */
    public static String getKafkaSinkDDL(String topic) {
        return " with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '" + topic + "',\n" +
                "    'properties.bootstrap.servers' = '" + Constant.KAFKA_SERVERS + "',\n" +
                "    'format' = 'json'\n" +
                ")";
    }

    /**
     * Upsert Kafka SQL 连接器 ,用于撤回流
     * @param topic
     * @return
     */
    public static String getUpsertKafkaSinkDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_SERVERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

}
