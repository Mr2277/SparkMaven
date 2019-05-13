package com.clear;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class hu {
    public static void main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("Reg");
        SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
        sparkSession.read().csv("file:///home/sun/Downloads/holder_shit.csv").registerTempTable("hu");
        sparkSession.read().csv("file:///home/sun/Music/test/D.csv").registerTempTable("y");
        //sparkSession.sql("select   concat_ws('#',collect_set(distinct trim(_c3))) as H_HCAPCTYPE ,concat_ws('#',collect_set(_c1)) as H_HCAPCCRY,concat_ws('#',collect_set(_c2)) as H_HCAPCAMT ,concat_ws('#',collect_set(_c4)) as H_HCAPCDATE  from hu group by _c0  having count(_c0)=1 ").registerTempTable("a");
       //System.out.println(sparkSession.sql("select * from "));
       //sparkSession.sql("select a.H_HCAPCTYPE ,y.* from a left join y on a.H_HCAPCTYPE=y._c0").registerTempTable("k");
       //sparkSession.sql("select * from k").show();
       //sparkSession.sql("select H_HCAPCTYPE from k where _c0 is null and _c1 is null").show(1000);//.repartition(1).write().csv("file:///home/sun/Downloads/rt");
        //sparkSession.sql("select y.*,a.* from y left join a on a.H_HCAPCTYPE=y._c0").show(2500);
        // sparkSession.read().csv("file:///home/sun/test.csv").registerTempTable("f");
        //System.out.println(sparkSession.sql("select * from a").count());
        //System.out.println(sparkSession.sql("select * from y").count());
        // sparkSession.sql("select concat_ws('#',collect_set( _c1)) from f group by _c0").show();




        /*
        sparkSession.sql("select _c0,  concat_ws('#',collect_set(distinct trim(_c3))) as H_HCAPCTYPE ,concat_ws('#',collect_set(_c1)) as H_HCAPCCRY,concat_ws('#',collect_set(_c2)) as H_HCAPCAMT ,concat_ws('#',collect_set(_c4)) as H_HCAPCDATE  from hu group by _c0  having count(_c0)=1 ").registerTempTable("a");
        Dataset<Row>dataset=sparkSession.sql("select a._c0,a.H_HCAPCCRY,a.H_HCAPCAMT,a.H_HCAPCDATE ,(case when y._c1 is null then '字段错误' else   y._c1 end)as new  from a left join y on a.H_HCAPCTYPE=y._c0");
        dataset.javaRDD().map((Function<Row,String>) revord -> {
            String str=revord.toString().substring(1,revord.toString().length()-1);
            String str1[]=str.split(",");
            if("字段错误".equals(str1[4])){
                return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+"," +  ""+","+""+","+""+","  +str1[1]+","+str1[2]+","+str1[3];
            }

             if("土地使用权".equals(str1[4])){
                return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+"," +  str1[1]+","+str1[2]+","+str1[3] + ""+","+""+","+"";
            }
             if("知识产权".equals(str1[4])){
                return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  str1[1]+","+str1[2]+","+str1[3]  +  ""+","+""+","+""+","+ ""+","+""+","+"";
            }
             if("实物".equals(str1[4])){
                return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+"," +  str1[1]+","+str1[2]+","+str1[3]  +  ""+","+""+","+""+","+  ""+","+""+","+""+","+ ""+","+""+","+"";
            }
             if("货币".equals(str1[4])){
                return str1[0]+","+""+","+""+","+""+","+  str1[1]+","+str1[2]+","+str1[3] +  ""+","+""+","+""+","+  ""+","+""+","+""+","+ ""+","+""+","+""+","+ ""+","+""+","+"";
            }
            if("混合".equals(str1[4])){
                return  str1[0]+","+str1[1]+","+str1[2]+","+str1[3]+  ""+","+""+","+""+","+  ""+","+""+","+""+","+ ""+","+""+","+""+","+""+","+""+","+""+","+ ""+","+""+","+"";
            }

                return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+"," + ""+","+""+","+""+","  + ""+","+""+","+"";

        }).repartition(1).saveAsTextFile("file:///home/sun/Music/result1");
        */

        sparkSession.sql("select _c0,  concat_ws('#',collect_set(_c3)) as H_HCAPCTYPE ,concat_ws('#',collect_set(_c1)) as H_HCAPCCRY,concat_ws('#',collect_set(_c2)) as H_HCAPCAMT ,concat_ws('#',collect_set(_c4)) as H_HCAPCDATE  from hu group by _c0  having count(_c0)>1 ").registerTempTable("a");
        Dataset<Row>dataset=sparkSession.sql("select a._c0,a.H_HCAPCCRY,a.H_HCAPCAMT,a.H_HCAPCDATE ,(case when y._c1 is null then '字段错误' else   y._c1 end)as new  from a left join y on a.H_HCAPCTYPE=y._c0");
         dataset.javaRDD().map((Function<Row,String>) revord -> {
         String result=revord.toString();
         String result1[]=result.split(",");
         String type[]=result1[4].split("#");
         String Hdate[]=result1[3].split("#");
         String Hamt[]=result1[2].split("#");
         if(type.length==1 && Hamt.length>1 && Hdate.length>1){
             return   result;
         }
         else{
             return  "";
         }
         /*
         for(int i=0;i<type.length;i++){
             if("字段错误".equals(type[i])){
                 return result1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+"," +  ""+","+""+","+""+","  +str1[1]+","+str1[2]+","+str1[3];
             }

             if("土地使用权".equals(str1[4])){
                 return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+"," +  str1[1]+","+str1[2]+","+str1[3] + ""+","+""+","+"";
             }
             if("知识产权".equals(str1[4])){
                 return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  str1[1]+","+str1[2]+","+str1[3]  +  ""+","+""+","+""+","+ ""+","+""+","+"";
             }
             if("实物".equals(str1[4])){
                 return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+"," +  str1[1]+","+str1[2]+","+str1[3]  +  ""+","+""+","+""+","+  ""+","+""+","+""+","+ ""+","+""+","+"";
             }
             if("货币".equals(str1[4])){
                 return str1[0]+","+""+","+""+","+""+","+  str1[1]+","+str1[2]+","+str1[3] +  ""+","+""+","+""+","+  ""+","+""+","+""+","+ ""+","+""+","+""+","+ ""+","+""+","+"";
             }
             if("混合".equals(str1[4])){
                 return  str1[0]+","+str1[1]+","+str1[2]+","+str1[3]+  ""+","+""+","+""+","+  ""+","+""+","+""+","+ ""+","+""+","+""+","+""+","+""+","+""+","+ ""+","+""+","+"";
             }

             return str1[0]+","+""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+","  +  ""+","+""+","+""+"," + ""+","+""+","+""+","  + ""+","+""+","+"";
         }
         */
         //return result;
        }).repartition(1).saveAsTextFile("file:///home/sun/Music/result2");
       /*
        for(String s:list){
            System.out.println(s);
        }
             */


        sparkSession.stop();
    }
}
