package com.atguigu.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author 城北徐公
 * @Date 2023/11/10-14:30
 */
public class KeywordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {

        ArrayList<String> words = new ArrayList<>();

        //创建IK分词器对象

        //ture:useSmart:贪婪模式,将尽可能多的字匹配成为一个词(词里面没有重复的字)[在, 尚, 硅谷, 的, 大, 数据, 0524, 班级, 使用, flink, 做, 实, 时数, 仓]
        //IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword),true);

        //false:ik_max_word:尽量拆分最多的词(词里面有重复的字)[在, 尚, 硅谷, 的, 大数, 数据, 0524, 班级, 班, 级, 使用, flink, 做, 实时, 时数, 仓]
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword),false);

        Lexeme lexeme = ikSegmenter.next();
        while ( lexeme != null) {
            String word = lexeme.getLexemeText();
            words.add(word);
            lexeme = ikSegmenter.next();
        }

        return words;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("在尚硅谷的大数据0524班级使用Flink做实时数仓"));
    }
}
