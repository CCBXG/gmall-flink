package com.atguigu.app.func;

import com.atguigu.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @Author 城北徐公
 * @Date 2023/11/10-14:25
 */
/**
 * 自定义切词函数UDTF
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class DwsSplitFunction extends TableFunction<Row> {
    public void eval(String keyword) {
        List<String> words = null;
        try {
            //注意:这个切词异常不能抛了,再抛就给sql了,处理不了
            words = KeywordUtil.splitKeyWord(keyword);
            for (String word : words) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            //如果切词异常,就不切了,直接全部返回
            collect(Row.of(words));
        }
    }
}
