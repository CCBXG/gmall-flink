package com.atguigu.util;

import com.atguigu.common.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

/**
 * @Author 城北徐公
 * @Date 2023/11/3-19:34
 * 获取kafkaSource
 */
public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {  //自定义序列化器，用于过滤掉空数据
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message == null){
                            return null;
                        }else {
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
}
