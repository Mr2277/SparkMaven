package com.neo4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestNeo4j {
    public static void main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("Neo4j");
        SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
        /*
        sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/px1.csv").registerTempTable("px1");
        //sparkSession.sql("select * from px1").show();
        //为数字列
        sparkSession.sql("select * from px1  where (_c4 REGEXP '[^0-9.]')=0").javaRDD().map((Function<Row,String>) record->{
            String str=record.toString().substring(1,record.toString().length()-1);
            String str1[]=str.split(",");
            String result=str1[0]+","+str1[1]+","+str1[2]+";"+str1[3]+","+str1[4]+","+"RMB";
            return result;
        }).repartition(1).saveAsTextFile("file:///home/first");

        //sparkSession.sql("select _c4,count(_c4) from px1 group by _c4").show(1000);
        /*

        sparkSession.sql("select * from px1").javaRDD().map((Function<Row,String>) record ->{
            String str=record.toString().substring(1,record.toString().length()-1);
            String str1[]=str.split(",");
          //  E_EID:ID(Entry),E_ENAME,E_STATUS,E_CAPAMT,E_CAPCRY,:Label
            if("E_EID".equals(str1[0])){
                return "E_EID:ID(Entry),E_ENAME,E_STATUS,E_CAPAMT,E_CAPCRY,:Label";
            }
            else {

                if("RMB".equals(str1[4])){

                }
            }
            return str;
        }).collect().forEach(s -> System.out.println(s));
            */
        /*
        sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/px2.csv").registerTempTable("persion");
        sparkSession.sql("select * from persion").javaRDD().map((Function<Row,String>) record ->{
            String str=record.toString().substring(1,record.toString().length()-1);
            String str1[]=str.split(",");
            if("E_EID".equals(str1[0])){
                return "E_EID:ID(Entry),E_ENAME,E_STATUS,E_CAPAMT,E_CAPCRY,:Label";
            }
            else{
                return str1[0]+","+str1[1]+","+str1[2]+","+str1[3]+","+str1[4]+","+"Persion";
            }
        }).repartition(1).saveAsTextFile("file:///home/Persion");
        */
        /*
        sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/PX1.csv").registerTempTable("Company");
        sparkSession.sql("select * from Company").javaRDD().map((Function<Row,String>) record->{
            String str=record.toString().substring(1,record.toString().length()-1);
            String str1[]=str.split(",");
            if("000000d9-9a86-4d55-95c3-14fbf18b8f6c".equals(str1[0])){
                String header="E_EID:ID(Entry),E_ENAME,E_STATUS,E_CAPAMT,E_CAPCRY,:Label";
                String result=str1[0]+","+str1[1]+","+str1[2]+","+str1[3]+","+str1[4]+","+"Company";
                return header+"\r\n"+result;
            }
            else{
                return str1[0]+","+str1[1]+","+str1[2]+","+str1[3]+","+str1[4]+","+"Company";
            }
        }).repartition(1).saveAsTextFile("file:///home/Company");
        */
        /*
        sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/qx_temp_02.csv").registerTempTable("re");
        sparkSession.sql("select * from re").javaRDD().map((Function<Row,String>) record->{
            String str=record.toString().substring(1,record.toString().length()-1);
            String str1[]=str.split(",");
            if("H_HID".equals(str1[0])){
                String header=":START_ID(Entry),:END_ID(Entry),:TYPE,H_HSHARE,H_HCAPCAMT,H_HCAPCCRY,H_HSDATE";

                return header;
            }
            else{
                return str1[0]+","+str1[1]+","+str1[2]+","+str1[3]+","+str1[4]+","+str1[5]+","+str1[6];
            }
        }).repartition(1).saveAsTextFile("file:///home/Relationship");
        */
         /*
        sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/Persion.csv").registerTempTable("P");
        sparkSession.sql("select * from P").distinct().registerTempTable("P");
        sparkSession.sql("select _c0 from P group by _c0 having count(*)>=2 ").show(10000);
        sparkSession.sql("select * from P where _c0 like '92C6AA71D724AF582%'").show();

        sparkSession.sql("select _c0 from P group by _c0").registerTempTable("t");
        sparkSession.sql("select count(*) from t").show();
       // sparkSession.sql()
        //sparkSession.sql("select count(distinct(_c0)) from P").show();

        /*
        sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/Company.csv").registerTempTable("Com");
        sparkSession.sql("select count(_c0) from Com").show();
        sparkSession.sql("select count(distinct(_c0)) from Com").show();
        */
         //sparkSession.read().csv("file:///home/econ_king_final/econ.csv").registerTempTable("e");
         //sparkSession.sql("select _c0,_c3 from e").repartition(1).write().csv("file:///home/MM");
        //sparkSession.read().csv("file:///home/MM/snleaves/").registerTempTable("snl");
        //sparkSession.sql("select * from snl");
        /*
        sparkSession.read().json("file:///home/MM/snleaves/").javaRDD().map((Function<Row,String>) record->{
            String str=record.toString().substring(1,record.toString().length()-1);
            String str1[]=str.split(",");
            StringBuilder builder=new StringBuilder();
            for(String s:str1){
                if(s!=null){

                    builder.append()
                }
            }
            return record.toString();
        });
        //sparkSession.sql("select * from s").show();
            */
        sparkSession.read().json("file:///home/allFinalHolderResults.json").javaRDD().map((Function<Row,String>) record ->{
            String str=record.toString().substring(1,record.toString().length()-1);
            return str;
        }).repartition(1).saveAsTextFile("file:///home/guo");
        sparkSession.stop();
    }
}
