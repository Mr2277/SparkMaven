package com.clear;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;

public class liu {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ClearBelong");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        /*
        sparkSession.read().csv("file:///home/sun/Documents/ENTERPRISE_REF.csv").registerTempTable("f");
        Dataset<Row> dataset=sparkSession.sql("select * from f");
        Properties properties=new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","1234567");
        properties.setProperty("driver","com.mysql.cj.jdbc.Driver");
        //properties.setProperty("driver","com.mysql.jdbc.Driver");
         dataset.write().jdbc("jdbc:mysql://127.0.0.1:3306/YMM?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT","ENTERPRISE_R",properties);
      //  dataset.write().jdbc("jdbc:mysql://10.168.7.245:3306/business_data_db?useUnicode=true&characterEncoding=utf-8","ename_status",properties);
      */
       // sparkSession.read().text("file:");
        sparkSession.stop();
    }
}
