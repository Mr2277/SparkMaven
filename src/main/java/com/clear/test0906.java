package com.clear;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test0906 {
    public static void main(String[] args) {
        String Comment_Txt = "";//读取用户评论
        String Phrase_Txt = "&NBSP;";//读取用户评论
        String Rst = "";
        String[] s = Phrase_Txt.split("，");
        for (int i = 0; i < s.length; i++) {

            char[] Sm = s[i].toCharArray();
            for (int j = 0; j < Sm.length; j++) {
                String m = "[" + Sm[j] + "]";
                Rst = Rst + m;
            }

            String regEx = "[`~!@#$%^&*()+=|{}:;\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？a-zA-Z 0-9]";
            //System.out.println(Comment_Txt.trim());

            Matcher m_data = ConverCompile(Comment_Txt.trim(), regEx);
            //System.out.println(m_data.find());
            //System.out.println(m_data.group());
            String result = m_data.replaceAll("").trim();
            System.out.println(result);
            System.out.println(Rst);

        }
    }
/*
  * 正则匹配
  */
        private static Matcher ConverCompile(String result,String regEx ){
            Pattern c = Pattern.compile(regEx);
            Matcher mc=c.matcher(result);
            return mc;

       }

}
