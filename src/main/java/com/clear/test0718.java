package com.clear;

import java.util.regex.Pattern;

public class test0718 {
    public static void main(String[] args) {



      String st="dfadfad";
     // st.substring()











        //String str = "\\\\\"\\\"fzjgdjjg\\\\\\\"\\\":\\\\\\\"\\\"522625\\\\\\\"\\\"\\\"\"";

        // \"fzjgdjjg\":\"522625\"
        /*
        String str="[{invest_type: 货币, should_capi_date: 2006-03-29, shoud_capi: 2万元人民币}]";
        String strArray[]=str.split(",");
        String should_capi=strArray[strArray.length-1];
        String should_capiArray[]=should_capi.split(":");
        //System.out.println(should_capiArray[1].substring(0,should_capiArray[1].length()-1));
        String should_capi_final=should_capiArray[1].substring(1,should_capiArray[1].length()-2);
        System.out.println(should_capi_final);
        */

       String str="[{\\invest_type\\: \\\\, \\should_capi_date\\: \\\\, \\shoud_capi\\: \\2.58万人民币\\}]";
       str=str.replace("\\","");
       System.out.println(str);
        //StringBuilder stringBuilder=new StringBuilder();
        /*
        for(int i=0;i<str.length();i++){
            char ch=str.charAt(i);
            if(ch == '[' ||ch ==']'){
                //stringBuilder.append(ch);
            }
            else{
                int flag=1;
                if(ch == '{'){
                  //  stringBuilder.append(ch);
                    //stringBuilder.append('"');
                    //flag=0;
                }
                if((ch >='a' && ch<='z') || ch == '_'){
                    stringBuilder.append(ch);
                    flag=0;
                }
                if(ch == ':'){
                    //stringBuilder.append('"');
                    stringBuilder.append(ch);
                    //stringBuilder.append('"');
                    flag=0;
                }
                if(ch == ','){
                    //stringBuilder.append('"');
                    stringBuilder.append(ch);
                    ///stringBuilder.append('"');
                    flag=0;
                }
                if(ch == '}'){
                    stringBuilder.append('"');
                    stringBuilder.append(ch);
                    flag=0;
                }
                if(flag ==1 && ch!= '\\'){
                    stringBuilder.append(ch);
                }
            }
        }
        */
        String s="54";
       double t= Double.parseDouble(s);
       String  string="[{invest_type: , real_capi: 160万人民币, real_capi_date: }]";
       System.out.println(string.contains("real_capi"));
       String stringArray[]=string.split(",");
       String real_capi="";
       for(String ss:stringArray){
           if(ss.contains("real_capi") && !ss.contains("real_capi_date")){
               real_capi=ss;
           }
       }
       //System.out.println(real_capi);
        String real_capi_final="";
        int last=0;
        if(real_capi !=null) {
            String real_capiArray[] = real_capi.split(":");
            last=real_capiArray.length-1;
             real_capi_final = real_capiArray[last];
        }


        //return pattern.matcher(string).matches();

        //System.out.println(stringBuilder.toString());
    }
}