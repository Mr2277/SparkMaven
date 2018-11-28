package com.mysql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MysqlBySpark {
    /*
    private static String driver = "com.mysql.cj.jdbc.Driver";
    private static String url = "jdbc:mysql://10.168.7.245:3306";  //hostname: mysql所在的主机名或ip地址
    private static String db = "business_data_db";  //dbname: 数据库名
    private static String user = "root"; //username: 数据库用户名
    private static String pwd = "zhirong123"; //password: 数据库密码；若无密码，则为""
    */
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("Mysql");
        SparkSession sparkSession=SparkSession.builder().config(conf).getOrCreate();

      /*  Dataset<Row>rowDataset=sparkSession.read().format("jdbc").option("url", url)
                .option("dbtable", db + "." + "anBang")
                .option("user", user)
                .option("password", pwd)
                .option("driver",driver)
                .load();
       // rowDataset.repartition(1).write().csv("file:///home/enterprise.csv");
        //Dataset<Row>rowDataset=sparkSession.sql("select * from enterprise limit 1000");
        rowDataset.show(100);
       sparkSession.stop();
       */
      sparkSession.read().json("file:///home/enterprise.json").registerTempTable("enterprise");
      Dataset<Row> dataset=sparkSession.sql("select * from enterprise ");
      //Dataset<Row> dataset=sparkSession.sql("select count(_c5),_c5 from enterprise group by _c5 ");
       //Dataset<Row>dataset=sparkSession.sql("select _c1 from enterprise");
       //Dataset<Row>dataset=sparkSession.sql("select * from enterprise where _c1='SD'");
      // Dataset<Row>dataset=sparkSession.sql("select _c2,count(_c2) from enterprise group by _c2 order by count(_c2) desc ");
        System.out.println(dataset.count());
        dataset.show();
       // dataset.repartition(1).write().csv("file:///home/status");
      sparkSession.stop();
    }
}
