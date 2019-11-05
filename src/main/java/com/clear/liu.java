package com.clear;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

public class liu {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ClearBelong");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        //sparkSession.read().option("multiLine", true).csv("file:///home/sun/Desktop/type(2).xlsx").registerTempTable("f");
        //Dataset<Row> dataset= sparkSession.sql("select _c0,count(_c0) from f group by _c0");
        //dataset.show(1000);
        //System.out.println(dataset.count());
       //dataset.repartition(1).write().csv("file:///home/sun/Desktop/econ");
        /*
        //dataset.repartition(1).write().csv("file:///home/sun/Downloads/db_enterprise_t_enterprise");
        dataset.show(1000);
        List<String> list=dataset.javaRDD().map((Function<Row,String>) revord -> {
              String str=revord.toString().substring(1,revord.toString().length()-1);
              String str1[]=str.split(",");
              StringBuilder stringBuilder=new StringBuilder();
              for(String s:str1){
                  stringBuilder.append('"');

              }
              return revord.toString();
        }).collect();
        for(String s:list){
            System.out.println(s);
        }
        String s="ddd";
        s.length();
        /*
        Properties properties=new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","zhirong123");
        properties.setProperty("driver","com.mysql.jdbc.Driver");
        //properties.setProperty("driver","com.mysql.jdbc.Driver");
         dataset.write().jdbc("jdbc:mysql://192.168.194.4:3306/F_CL_ENTERPRISE_DB?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT","db_enterprise_t_change_records",properties);
      //  dataset.write().jdbc("jdbc:mysql://10.168.7.245:3306/business_data_db?useUnicode=true&characterEncoding=utf-8","ename_status",properties);

       // sparkSession.read().text("file:");
          */
        sparkSession.read().option("multiLine", true).csv("file:///media/sun/DATA/t_enterprise_0.csv").registerTempTable("t_enter");
        sparkSession.read().option("multiLine", true).csv("file:///home/sun/Desktop/enterprise_new.csv").registerTempTable("enter");
        //Dataset<Row> dataset= sparkSession.sql("select a.*,b.* from t_enter a,enter b where a._c0=b._c0");
        //dataset.show();
        Dataset<Row> dataset= sparkSession.sql("select distinct(_c0) from t_enter");
        System.out.println(dataset.count());
        sparkSession.stop();
    }
}
