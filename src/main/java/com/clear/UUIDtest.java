package com.clear;

import java.math.BigDecimal;
import java.util.UUID;

public class UUIDtest {
    public static void main(String[] args){

        String str="大华银行(中国)有限公司";
        /*
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
      double num=Double.parseDouble(str);
      nf.format(num*1000000).toString();
      String result="";
      if(!"".equals(str))
      */
        System.out.println(str.contains("银行") );
        System.out.println(str.length());


        String uuid = UUID.randomUUID().toString();
        System.out.println(uuid);






    }
}
