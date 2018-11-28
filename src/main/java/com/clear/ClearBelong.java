package com.clear;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;

import java.util.HashMap;

public class ClearBelong {
    public static void main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("ClearBelong");
        SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
        //String s="";
       // sparkSession.stop();
        //sparkSession.read().csv("file:///home/region.csv").registerTempTable("region");
        //Dataset<Row>dataset=sparkSession.sql("select substr(_c0,1,4) as Reg,count(substr(_c0,1,4)),_c1 from region group by substr(_c0,1,4)");
        //sparkSession.read().csv("file:///home/enterprise_clear_belongorg.csv").registerTempTable("belong");
        //sparkSession.read().csv("file:///home/region.csv").registerTempTable("region");
      // Dataset<Row>dataset=sparkSession.sql("select _c0,substr(_c0,1,4),_c3,_c4,_c5,_c6 from belong");
       //Dataset<Row>dataset1=sparkSession.sql("select * from region");
       //Dataset<Row>dataset1=dataset.map
        //Dataset<Row>dataset=sparkSession.sql("select length(_c0),_c0 from region ");
        //sparkSession.sql("select _c0 ,_c1 ,_c3  ,_c4 ,_c5,_c6,(case when length(_c1) =13 or length(_c1)=15 then substr(_c1,1,6) when length(_c1)=18 then substr(_c1,3,6) end )as _c7 from belong").registerTempTable("belong1");
      // Dataset<Row>dataset=sparkSession.sql("select * from belong1");
        //Dataset<Row>dataset=sparkSession.sql("select belong1._c0 as uid,belong1._c1 as reg_no,belong1._c3 as credit_no ,region.*,belong1._c4 as adress,belong1._c5 as belong_org,belong1._c6 as reg_adress from belong1 left join region on belong1._c7=region._c4");
        //Dataset<Row>dataset=sparkSession.sql("select * from region");
       // dataset.show();
        //dataset.repartition(1).write().csv("file:///home/R");

        //sparkSession.read().csv("file:///home/R/part-00000-136703f1-feba-4a2c-927c-eef9f7124007.csv").registerTempTable("belong_clear");
       // sparkSession.sql("select * from belong_clear where _c3  is  null").repartition(1).write().csv("file:///home/Error");
        //sparkSession.sql("select count(1) from belong_clear where _c3 is not null").show();


        //sparkSession.read().csv("file:///home/Error/part-00000-c2fea592-ecdc-490e-840e-fdf14791c4c6.csv").registerTempTable("error");
        //sparkSession.sql("select count(1) from error").show();
        /*
        sparkSession.read().csv("file:///home/s.csv").registerTempTable("s");
        Dataset<Row>dataset=sparkSession.sql("select * from s");
        JavaRDD<Row>javaRDD=dataset.javaRDD();
        javaRDD.map((Function<Row,String>) revord ->{
                //System.out.println("dddddddddddd");
                String result=revord.toString().substring(1,revord.toString().length()-1);
                String str[]=result.split(",");
                HashMap<Integer,Integer> hashMap=new HashMap<>();
                for(int i=1;i<=9;i++){
                    hashMap.put(i,i*1000);
                }
                String Re=result;
                for(int i=1;i<str.length;i++){
                    if(i==1){
                           if(!"null".equals(str[i]) && "null".equals(str[0])){
                                int a=Integer.parseInt(str[i])/1000;
                                String first=hashMap.get(a).toString();
                                Re= first+","+str[1]+","+str[2]+","+str[3]+","+str[4];
                                break;
                            }
                    }
                    if(i==2){
                            if(!"null".equals(str[i])){

                                int a=Integer.parseInt(str[i])/1000;
                                int b=(Integer.parseInt(str[i])/100)%10;
                                int c=hashMap.get(a)+b*100;
                                //return
                                //return "fuck";
                                Re=hashMap.get(a)+","+c+","+str[2]+","+str[3]+","+str[4];
                                break;

                               // Re=str[i];
                            }
                        }
                        if(i==3){
                            if(!"null".equals(str[i])){
                                int a=Integer.parseInt(str[i])/1000;
                                int b=(Integer.parseInt(str[i])/100)%10;
                                int c=(Integer.parseInt(str[i])/10)%10;
                                int B=b*100+hashMap.get(a);
                                int C=B+c*10;
                                Re=hashMap.get(a)+","+B+","+C+","+str[i]+","+str[4];
                            }
                        }
                        //return revord.toString().substring(1,revord.toString().length()-1);


                }
                return Re;
                }).repartition(1).saveAsTextFile("file:///home/type");
                 */
            //sparkSession.read().csv("file:///home/enterprise_econkind.csv").registerTempTable("econkind");
           // sparkSession.read().csv("file:///home/type.csv").registerTempTable("type");
            //Dataset<Row>dataset=sparkSession.sql("select * from econkind");
            //Dataset<Row>dataset1=sparkSession.sql("select * from type");
            //Dataset<Row>dataset2=sparkSession.sql("select econkind._c0 as uid,econkind._c1 as econkind,econkind._c2 as province,econkind._c3 as category ,econkind._c4 as parent_domains, econkind._c5 as belong_org,econkind._c6 as reg_address          ,type.* from econkind left join type on econkind._c1=type._c4");
            //dataset2.repartition(1).write().csv("file:///home/econ");
             //dataset.show();
            // dataset2.show();
            //sparkSession.read().csv("file:///home/full.csv").registerTempTable("full");
           // Dataset<Row>dataset=sparkSession.sql("select a.*,b.* from full a left join full b on a._c0=b._c0");
            //System.out.println(dataset.count());
            // dataset.repartition()
          // sparkSession.read().csv("file:///home/enterprise_clear_belongorg.csv").registerTempTable("enterprise");
          // sparkSession.read().csv("file:///home/enterprise_part.csv").registerTempTable("part");
           //Dataset<Row>dataset=sparkSession.sql("select * from enterprise");
           //Dataset<Row>dataset1=sparkSession.sql("select * from part");
           //Dataset<Row>dataset2=sparkSession.sql("select enterprise.*,part.* from enterprise left join part on enterprise._c0=part._c0");
           //dataset.show();
           //dataset1.show();
           //sparkSession.read().csv("file:///home/address.csv").registerTempTable("address");
          // sparkSession.read().csv("file:///home/econ/part-00000-704bc2fb-7fce-49c4-99b8-67043d77ff6d.csv").registerTempTable("econ");
          //sparkSession.sql("select * from address").show();
        //sparkSession.sql("select * from econ").show();
          // Dataset<Row>rowDataset=sparkSession.sql("select econ._c0 as uid,econ._c7 as level_1, econ._c8 as level_2, econ._c9 as level_3 ,econ._c10 as level_10, econ._c3 as category ,econ._c5 as belong_org, address._c7 as E_LOCALID,econ._c2 as province ,address._c6 as CITY,econ._c6 as AD from econ left join address on address._c0=econ._c0");
           //dataset.show();
          // dataset1.show();
        //rowDataset.show();
         //  Dataset<Row>dataset1=sparkSession.sql("select address._c0 as uid,address._c7 as E_LOCALID, address._c6 as CITY from address");
           //dataset1.show();
         // Dataset<Row>dataset=sparkSession.sql("select econ._c0 as eid,address._c0 as aid from econ left join address on econ._c0=address._c0");
         // dataset.show();
          //System.out.println(dataset.count());
         //Dataset<Row>dataset=sparkSession.sql("select count(length(econ._c0)),length(econ._c0) from econ group by length(econ._c0)");
         //dataset.show(100);
         //System.out.println(dataset.count());
        //Dataset<Row>dataset=sparkSession.sql("select count(address._c0) from  address ");
        //dataset.show();
        //System.out.println(dataset.count());
        sparkSession.read().csv("file:///home/reg_no.csv").registerTempTable("reg");
       // Dataset<Row>dataset=sparkSession.sql("select count(1) from reg");
       sparkSession.sql("select count(length(reg._c0)) as _c1,length(reg._c0)as _c2 from reg group by length(reg._c0)").registerTempTable("aa");
       Dataset<Row>dataset=sparkSession.sql("select sum(_c1) from aa");
       dataset.show(1000);
        //System.out.println(dataset.count());
        //sparkSession.read().csv("")
        sparkSession.stop();

    }
}
