package com.atguigu.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;

public class DorisUtil {

    public static DorisSink<String> getDorisSink(String tableName) {

        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("hadoop102:7030")
                        .setTableIdentifier(tableName)
                        .setUsername("root")
                        .setPassword("000000")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setStreamLoadProp(properties)
                        .disable2PC()
                        .setMaxRetries(3)
                        .setCheckInterval(5000)
                        .setBufferSize(1024)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();

    }

}
