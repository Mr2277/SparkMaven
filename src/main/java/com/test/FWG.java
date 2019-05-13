package com.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class FWG {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("ClearBelong");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        sparkSession.read().csv("file:///media/sun/Samsung_T5/FWG_0422.csv").registerTempTable("a");
       Dataset<Row>dataset=sparkSession.sql("select (case when a._c11 is not null then a._c11 else a._c14 end ) as ID , (case when a._c12 is not null then a._c12 else a._c27 end ) as H_NAME, a._c1 as H_ETYPE,a._c16 as H_HCATEGORY, a._c17  as H_HIDTYPE,a._c18  as H_HTYPE0, a._c19   as H_HTYPE1, a._c20   as H_HTYPE2, a._c21  as H_HTYPE3, a._c22  as H_HTYPE4, a._c23  as H_HCLASS, a._c24 as H_HNAMEENT from a");
          dataset.registerTempTable("b");
          Dataset<Row>dataset1=sparkSession.sql("SELECT\n" +
                  "\tID,\n" +
                  "\t      concat_ws('#',collect_set(H_ETYPE))   AS H_ETYPE,\n" +
                  "\t\t  concat_ws('#',collect_set( H_NAME))  AS H_NAME,\n" +
                  "\n" +
                  "\t      concat_ws('#',collect_set(  H_HCATEGORY))       AS H_HCATEGORY,\n" +
                  "\t\t         concat_ws('#',collect_set(  H_HIDTYPE))                AS H_HIDTYPE,\n" +
                  "\n" +
                  "\t        concat_ws('#',collect_set(  H_HTYPE0))                         AS H_HTYPE0,\n" +
                  "\t       concat_ws('#',collect_set(  H_HTYPE1))                          AS H_HTYPE1,\n" +
                  "\tconcat_ws('#',collect_set(  H_HTYPE2))   AS H_HTYPE2,\n" +
                  "\t               concat_ws('#',collect_set(  H_HTYPE3))       AS H_HTYPE3,\n" +
                  "\tconcat_ws('#',collect_set(  H_HTYPE4))               AS H_HTYPE4,\n" +
                  "\t\tconcat_ws('#',collect_set(  H_HCLASS))   AS H_HCLASS,\n" +
                  "\t\t     concat_ws('#',collect_set(  H_HNAMEENT))                  AS H_HNAMEENT\n" +
                  "\n" +
                  "FROM\n" +
                  "\tb\n" +
                  "GROUP BY\n" +
                  "\tID ");


        Properties properties=new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","zhirong123");
        properties.setProperty("driver","com.mysql.jdbc.Driver");
        dataset1.write().jdbc("jdbc:mysql://192.168.194.4:3306/F_CL_ENTERPRISE_DB?useUnicode=true&characterEncoding=utf-8","t0424HIDEID",properties);




        //dataset1.repartition(1).write().csv("file:///home/sun/Pictures/FWGMORE");
        sparkSession.stop();
    }
}
