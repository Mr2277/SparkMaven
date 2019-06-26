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
        sparkSession.read().option("multiLine", true).csv("file:///media/sun/Samsung_T5/dataFromSZ20190621/db_enterprise_t_enterprise").registerTempTable("f");
        Dataset<Row> dataset= sparkSession.sql("select * from f ");
        dataset.repartition(1).write().csv("file:///home/sun/Downloads/db_enterprise_t_enterprise");
        //dataset.show(1000);
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

        sparkSession.stop();
    }
}
