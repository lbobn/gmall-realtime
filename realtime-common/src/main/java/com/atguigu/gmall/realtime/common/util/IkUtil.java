package com.atguigu.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @description
 * @Author lubb
 * @create 2024-03-15 21:21
 */
public class IkUtil {
    /**
     * IK分词器
     * @param s 需要分词的串
     * @return  分好词的列表
     */
    public static List<String> Split(String s) {

        StringReader stringReader = new StringReader(s);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = null;
        ArrayList<String> list = new ArrayList<>();
        try {
            next = ikSegmenter.next();
            while (next != null) {
                list.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return list;
    }
}
