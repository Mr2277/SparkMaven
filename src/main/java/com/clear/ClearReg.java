package com.clear;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Properties;

public class ClearReg {
    public static void main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("Reg");
        SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
                 /*
                 Dataset<Row> dataset1=sparkSession.read().csv("file:///home/test1.txt");
                //.union(sparkSession.sql(""))
                 Dataset<Row> dataset2=sparkSession.read().csv("file:///home/test2.txt");
                 Dataset<Row> dataset3=sparkSession.read().csv("file:///home/test3.txt");
                 Dataset<Row> dataset4=sparkSession.read().csv("file:///home/test4.txt");
                 Dataset<Row> dataset5=sparkSession.read().csv("file:///home/test5.txt");
                 Dataset<Row> dataset6=sparkSession.read().csv("file:///home/test6.txt");
        Dataset<Row> dataset7=sparkSession.read().csv("file:///home/test7.txt");
        Dataset<Row> dataset8=sparkSession.read().csv("file:///home/test8.txt");
        Dataset<Row> dataset9=sparkSession.read().csv("file:///home/test9.txt");
        Dataset<Row> dataset10=sparkSession.read().csv("file:///home/test10.txt");
        Dataset<Row> dataset11=sparkSession.read().csv("file:///home/test11.txt");
        Dataset<Row> dataset12=sparkSession.read().csv("file:///home/test12.txt");
        Dataset<Row> dataset13=dataset1.union(dataset2).union(dataset3).union(dataset4).union(dataset5).union(dataset6).union(dataset7).union(dataset8).union(dataset9).union(dataset10).union(dataset11).union(dataset12);
        dataset13.registerTempTable("dataset13");
       // sparkSession.sql("select * from dataset13 where length(_c0) = 36").registerTempTable("fuck");
       // sparkSession.sql("select * from fuck").show();
       //   sparkSession.sql("select _c0,_c1 from fuck where length(_c1) =2 ").show(1000);
       // sparkSession.sql("select _c0,_c1,(case when length(_c1) =13 or length(_c1)=15 then substr(_c1,1,6) when length(_c1)=18 then substr(_c1,3,6) end )as _c7 from fuck").show();
        //  sparkSession.sql("select _c1 from fuck where length(_c1) = 13 or length(_c1)=15 or length(_c1)=18").registerTempTable("aa");
         // sparkSession.sql("select count(length(_c1)),length(_c1) from fuck group by length(_c1) order by length(_c1)").show(1000);
        // sparkSession.sql("select (case when length(_c1) =13 or length(_c1)=15 then substr(_c1,1,6) when length(_c1)=18 then substr(_c1,3,6) end ) as _c2 from fuck").registerTempTable("b");
         //sparkSession.sql("select * from b").show();
        //sparkSession.sql("select count(length(_c2)),length(_c2) from b group by length(_c2)").show();
         //sparkSession.sql("select * from fuck where length(_c1) =1").show();

        //sparkSession.read().csv("file:///home/reg_uid_from_mysql.csv").registerTempTable("reg");
        //sparkSession.read().csv("file:///home/");
       // sparkSession.sql("select count(1) from reg").show();
        //sparkSession.sql("select count(length(_c1)),length(_c1) from reg group by length(_c1)").show();
        sparkSession.sql("select * from dataset13 where length(_c1) =16 or length(_c1)=14 or length(_c1)=19").registerTempTable("reg1");
        sparkSession.sql("select *,(case when length(_c1) =14 or length(_c1)=16 then substr(_c1,1,6) when length(_c1)=19 then substr(_c1,3,6) end ) as _c2  from reg1").registerTempTable("reg2");
        //dataset.show();
        sparkSession.read().csv("file:///home/region.csv").registerTempTable("region");
        sparkSession.sql("select * from region").show();
        sparkSession.read().csv("file:///home/fuck.csv").registerTempTable("fuck");
       sparkSession.sql("select reg2._c0 as uuid, reg2._c1 as reg_no,region._c0 as P_ID,region._c1 as E_PROVINCEE,region._c3 as E_CITY,region._c5 as E_REGION from reg2 left join region on region._c4 =reg2._c2").registerTempTable("fuck2");
        //dataset14.show(1000);
       // System.out.println(dataset14.count());
       // rowDataset.show();
        Dataset<Row>dataset=sparkSession.sql("select fuck2.uuid ,fuck2.reg_no,fuck2.E_PROVINCEE,fuck2.E_CITY,fuck2.E_REGION,fuck._c2 from fuck2 left join fuck on fuck._c1 = fuck2.P_ID");
        //dataset.show();
       dataset.repartition(1).write().csv("file:///home/reg_no_final");
       */
              //   sparkSession.read().csv("file:///home/econ_from_mysql.csv").registerTempTable("econ");
                // Dataset<Row>dataset=sparkSession.sql("select * from econ");
                 //dataset.show();
                 //sparkSession.sql("select count(1) from econ").show();
        /*
        Dataset<Row>dataset1=sparkSession.read().csv("file:///home/e1.txt");
        Dataset<Row>dataset2=sparkSession.read().csv("file:///home/e2.txt");
        Dataset<Row>dataset3=sparkSession.read().csv("file:///home/e3.txt");
        Dataset<Row>dataset4=sparkSession.read().csv("file:///home/e4.txt");
        Dataset<Row>dataset5=sparkSession.read().csv("file:///home/e5.txt");
        Dataset<Row>dataset6=sparkSession.read().csv("file:///home/e6.txt");
        Dataset<Row>dataset7=sparkSession.read().csv("file:///home/e7.txt");
        Dataset<Row>dataset8=sparkSession.read().csv("file:///home/e8.txt");
        Dataset<Row>dataset9=sparkSession.read().csv("file:///home/e9.txt");
        Dataset<Row>dataset10=sparkSession.read().csv("file:///home/e10.txt");
        Dataset<Row>dataset11=sparkSession.read().csv("file:///home/e11.txt");
        Dataset<Row>dataset12=sparkSession.read().csv("file:///home/e12.txt");
        Dataset<Row>dataset13=dataset1.union(dataset2).union(dataset3).union(dataset4).union(dataset5).union(dataset6).union(dataset7).union(dataset8).union(dataset9).union(dataset10).union(dataset11).union(dataset12);
        dataset13.registerTempTable("dataset13");
        sparkSession.sql("select * from dataset13 where length(_c0)=36").registerTempTable("dataset14");
        //sparkSession.sql("select * from dataset14").show();
        sparkSession.read().csv("file:///home/type.csv").registerTempTable("type");
        //sparkSession.sql("select * from type").show();

        //Dataset<Row>rowDataset=sparkSession.sql("select dataset14._c0 as uuid,dataset14._c1 as econ_kind ,dataset14._c2 as category,type._c0,type._c1,type._c2,type._c3    from dataset14 left join type on dataset14._c1 = type._c4");
       //System.out.println(rowDataset.count());
        sparkSession.sql("select dataset14._c0 as uuid,dataset14._c1 as econ_kind ,dataset14._c2 as category,type._c0,type._c1,type._c2,type._c3    from dataset14 left join type on dataset14._c1 = type._c4").registerTempTable("a");
        sparkSession.sql("select * from a ").repartition(1).write().csv("file:///home/econ_king_final");
        ///sparkSession.sql("select * from a").show(1000);
        //
        */
        /*
        sparkSession.read().csv("file:///home/parent_domains_from_mysql.csv").registerTempTable("domains");
        sparkSession.sql("select * from domains").show();
        sparkSession.read().csv("file:///home/ff.csv").registerTempTable("ff");
        sparkSession.sql("select * from ff").show();
        sparkSession.sql("select domains._c0 as uuid ,domains._c1 as domains,ff._c0 as L from domains left join ff on domains._c1=ff._c1").registerTempTable("a");
        sparkSession.sql("select * from a").repartition(1).write().csv("file:///home/domain_final");
        //sparkSession.sql("select count(1) from a where domains is not null").show();
        //sparkSession.sql("select count(1) from a where domains is null").show();
        //sparkSession.sql("select count(uuid) from a").show();
        */



        //sparkSession.sql("select id_name._c0 as id,id_name._c1 as name, domains._c1 from id_name left join domains on id_name._c0 = domains._c0").repartition(1).write().csv("file:///home/id_name_domains");
        //sparkSession.sql("select count(_c0) from id_name").show();


        //sparkSession.read().text("file:///home/test/").show();
        //sparkSession.read().text("file:///home/test/2.txt").show();
        /*
        sparkSession.read().csv("file:///home/econ_king_final/econ.csv").registerTempTable("econ");
        sparkSession.sql("select * from econ").show();
        sparkSession.sql("select count(1) from econ where _c3 is null and _c4 is null and _c5 is null and _c6 is null").show();
        */
        /*
        sparkSession.read().text("file:///home/AA").registerTempTable("aa");
        //sparkSession.read().text("file:///home/0.txt").registerTempTable("aa");
        sparkSession.sql("select * from aa").javaRDD().map((Function<Row,String>) revord ->{
            String result=revord.toString().substring(1,revord.toString().length()-1);
            /*
            String str[]=result.split(" ");
            if(str.length ==3) {
                return str[0] + "," + str[1] + "," + str[2];
            }
            else{
                return str[0];
            }

            return result;
        }).repartition(1).saveAsTextFile("file:///home/zhan_final");
          */

        //sparkSession.read().csv("file:///home/Id_name_from_mysql.csv").registerTempTable("id_name");
        /*
         sparkSession.read().csv("file:///home/domain_final/domains.csv").registerTempTable("domains");
         sparkSession.read().csv("file:///home/econ_king_final/econ.csv").registerTempTable("econ");
         //sparkSession.sql("select count(1) from domains").show();
        sparkSession.sql(" select * from econ where  _c3=1000 and _c4=1100  and _c5 is null and _c6 is null").registerTempTable("econ");
        sparkSession.sql("select * from econ").show();
        sparkSession.sql("select count(1) from econ").show();
       // sparkSession.sql("select count(1) from domains").show();


        sparkSession.read().csv("file:///home/reg_no_final/reg_no.csv").registerTempTable("reg_no");
        sparkSession.read().csv("file:///home/dataDemo").registerTempTable("dataDemo");
        sparkSession.sql("select * from dataDemo").show();
        //sparkSession.sql("select count(1) from dataDemo").show();
        //sparkSession.sql("select * from id_name").show();
        //sparkSession.sql("select * from domains").show();

        /*
        sparkSession.sql("select id_name._c0 as uid ,id_name._c1 as name,domains._c1 as parent_domains,domains._c2 as category from id_name left join domains on id_name._c0=domains._c0 ").registerTempTable("union1");
        //sparkSession.sql("select * from econ").show();
        sparkSession.sql("select union1.*,econ._c1 as econ_kind, econ._c2 as C,econ._c3 as first ,econ._c4 as second ,econ._c5 as third ,econ._c6 as four from union1 left join econ on union1.uid = econ._c0").repartition(1).write().csv("file:///home/union2");//r.registerTempTable("union2");
        //sparkSession.sql("select * from reg_no").show();
           */

        //sparkSession.read().csv("file:///home/union3/union3.csv").registerTempTable("union3");
        //sparkSession.sql("select * from union2").show();
       // sparkSession.sql("select union2.* ,reg_no._c1 as reg_num,reg_no._c3 as E_CITY, reg_no._c5 as province from union2 left join reg_no on union2._c0=reg_no._c0").repartition(1).write().csv("file:///home/union3");
        //sparkSession.sql("select * from union3 ").show();
        //sparkSession.sql("select count(distinct _c0) from union3").show();
       // sparkSession.sql("select count(length(_c0)),length(_c0) from union3 group by length(_c0)").show();
       // sparkSession.sql("select * from union3 where length(_c0) != 36").show(1000);



        //sparkSession.sql("select count(1) from id_name ").show();
        //sparkSession.sql("select count(1) from id_name where length(_c0) = 36").show();
        //sparkSession.sql("select count(1) from domains").show();
       // sparkSession.sql("select count(1) from econ").show();
       //sparkSession.sql("select count(1) from reg_no").show();
      //  sparkSession.read().csv("file:///home/parent_domains_from_mysql.csv").registerTempTable("domains");
       // sparkSession.sql("select count(1) from domains").show();
         /*
        sparkSession.read().csv("file:///home/reg_no/reg.csv").registerTempTable("reg_no");
        sparkSession.read().csv("file:///home/dataDemo").registerTempTable("dataDemo");
        sparkSession.sql("select * from reg_no").show();
        sparkSession.sql("select * from dataDemo").show();
        sparkSession.sql("select count(distinct(_c0)) from dataDemo ").show();
        //sparkSession.sql("select _c0, (case when length(_c1) =13 or length(_c1)=15 then substr(_c1,1,6) when length(_c1)=18 then substr(_c1,3,6) end ) as _c2 ,_c5,_c3 from reg_no").registerTempTable("reg_no");
       //sparkSession.sql("select dataDemo.*, reg_no._c1 as E_LOCID,reg_no._c2 as E_P, reg_no._c3 as E_CITY  from dataDemo left join reg_no on dataDemo._c0=reg_no._c0").registerTempTable("union1");
        //sparkSession.sql("select * from reg_no ").repartition(1).write().csv("file:///home/reg_no11111");
     //  Dataset<Row>dataset= sparkSession.sql("select * from union1 ");//.repartition(1).write().csv("file:///home/union1");
        //  dataset.repartition(1).write().csv("file:///home/union1");

          //sparkSession.read().csv("file:///home/econ_king_final/econ.csv").registerTempTable("econ");
          //sparkSession.sql("select * from econ").show();
        //  sparkSession.sql("select _c0,_c1 from econ group by _c0,_c1").registerTempTable("econ");
        //  sparkSession.sql("select count(_c1),_c1 from econ group by _c1").repartition(1).write().csv("file:///home/tongji");
       // sparkSession.read().csv("file:///home/union1/")
        /*
        sparkSession.read().csv("file:///home/renminbi_final/part.csv").registerTempTable("renminbi");
        sparkSession.sql("select * from renminbi").show();
        */

        // sparkSession.read().csv("file:///home/union1/union1.csv").registerTempTable("union1");
      //   sparkSession.read().csv("file:///home/renminbi_final/part.csv").registerTempTable("renminbi");
         //sparkSession.sql("select * from union1").show();
         //sparkSession.sql("select count(1) from union1").show();
      //  sparkSession.sql("select count(distinct(_c0)) from union1").show();
      //  sparkSession.sql("select  count(distinct(_c0)) from reminbi").show();
       // sparkSession.sql("select union1.*,renminbi._c1 as r,renminbi._c2 as R from union1 left join renminbi on union1._c0 = renminbi._c0").repartition(1).write().csv("file:///home/union2");

         ///写入mysql
        /*
        sparkSession.read().csv("file:///home/union3/union3.csv").registerTempTable("union");
        Dataset<Row>dataset=sparkSession.sql("select * from union");
        Properties properties=new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","1234567");
        properties.setProperty("driver","com.mysql.cj.jdbc.Driver");

        dataset.write().jdbc("jdbc:mysql://10.168.7.231:3306/spark?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT","enterprise",properties);
          */
        //sparkSession.read().csv("file:///home/statusDemo").registerTempTable("statusDemo");
        //sparkSession.read().csv("file:///home/ys.txt").registerTempTable("ys");
        //sparkSession.sql("select * from statusDemo").show();
        //sparkSession.sql("select * from ys").show();
        //sparkSession.sql("select statusDemo.* ,ys._c1 as yyy from statusDemo left join ys on statusDemo._c1=ys._c0").registerTempTable("stat");
        //sparkSession.sql("select stat.uid,(case when _c0)")
        //sparkSession.sql("select * from stat").show(1000);
        //sparkSession.sql("select stat._c0, (case when yyy is null then _c1 else yyy end) as _c3 from stat ").repartition(1).write().csv("file:///home/stat_final");
        //sparkSession.read().csv("file:///home/stat_final/stat.csv").registerTempTable("stat");
        //sparkSession.read().csv("file:///home/union2/union.csv").registerTempTable("union");
        //sparkSession.sql("select union.*,stat._c1 as t from union left join stat on union._c0 =stat._c0").repartition(1).write().csv("file:///home/union3");

        ///mysql  to  server
        /*
        sparkSession.read().csv("file:///home/union3/union3.csv").registerTempTable("union3");
        Dataset<Row> dataset=sparkSession.sql("select _c1,_c16 from union3");
        Properties properties=new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","zhirong123");
        //properties.setProperty("driver","com.mysql.cj.jdbc.Driver");
        properties.setProperty("driver","com.mysql.jdbc.Driver");
       // dataset.write().jdbc("jdbc:mysql://10.168.7.245:3306/spark?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT","ename_status",properties);
        dataset.write().jdbc("jdbc:mysql://10.168.7.245:3306/business_data_db?useUnicode=true&characterEncoding=utf-8","ename_status",properties);
        //sparkSession.read().csv("file:///home/reg_no/reg.csv").registerTempTable("reg_no");
        //sparkSession.sql("select * from reg_no").show(1000);
           */
        //sparkSession.read().csv("file:///home/union3/union3.csv").registerTempTable("union3");
        //sparkSession.sql("select _c0 from union3 group by _c0 having count(_c0)>=2").repartition(1).write().csv("file:///home/_c0");
       // sparkSession.sql("select count(distinct(_c0)) from union3").show();
       // sparkSession.read().csv("file:///home/domainTest").registerTempTable("domain");
        //sparkSession.sql("select count(1) from domain").show();
       // sparkSession.sql("select * from domain").show();
        // sparkSession.sql("select _c0,concat_ws(',',collect_set(_c1))as TT, concat_ws(',',collect_set(_c2))as T from domain group by _c0").registerTempTable("domain");
       // sparkSession.sql("select _c0,count(_c0) from domain group by _c0 having count(_c0) >=2").show();
        ///sparkSession.sql("select count(1) from domain").show();
      //  sparkSession.sql("select union3.*,domain.TT,domain.T from union3 left join domain on union3._c0 = domain._c0").repartition(1).write().csv("file:///home/union4");
        //sparkSession.read().csv("file:///home/union4/union4.csv").registerTempTable("union4");
        //sparkSession.sql("select count(1) from union4").show();
        //sparkSession.read().csv("file:///home/qx_temp_02.csv").registerTempTable("qx_01");
       // sparkSession.sql("select length(_c0),count(length(_c0)) from qx_01 group by length(_c0)").show(10000);
       // sparkSession.sql("select * from qx_01 where length(_c0) =36" ).repartition(1).write().csv("file:///home/qx01");

        // sparkSession.sql("select * from qx_01 where length(_c0) =5" ).show();

        //sparkSession.read().csv("file:///home/neo4j-community-3.4.10/bin/p02.csv").registerTempTable("qx_02");
       // sparkSession.sql("select length(_c0),count(length(_c0)) from qx_02 group by length(_c0)").show(1000);
        /*
        sparkSession.sql("select * from qx_02 where length(_c0) = 11").show();
        sparkSession.sql("select * from qx_02 where length(_c0) = 10").show();
        sparkSession.sql("select * from qx_02 where length(_c0) = 15").show();
        sparkSession.sql("select * from qx_02 where length(_c0) = 5").show();
         */
        //sparkSession.sql("select * from qx_02 where length(_c0)=32 ").repartition(1).write().csv("file:///home/qx_02");
        //sparkSession.read().csv("file:///home/qx_temp_01_01.csv").registerTempTable("qx_1");
        //sparkSession.sql("select * from qx_1 where length(_c0)=36 or length(_c0)=5").repartition(1).write().csv("file:///home/ttt");

        //sparkSession.sql("select * from qx_2 where length(_c0)=32 or _c0='E_EID'").registerTempTable("qx_2");//.repartition(1).write().csv("file:///home/fffff");
        //sparkSession.sql("select * from qx_2 ").repartition(1).write().csv("file:///home/ttt");

        //sparkSession.read().csv("file:///home/union4/union4.csv").registerTempTable("union4");
       // sparkSession.sql("select * from union4").show();
        //sparkSession.sql("select count(*) from union4").show();
        //sparkSession.read().csv("file:///home/dateNewDemo/").registerTempTable("dateNewDemo");
        //sparkSession.sql("select * from dateNewDemo").show();
        //sparkSession.sql("select count(*) from dateNewDemo").show();
        //sparkSession.sql("select union4.*,dateNewDemo._c1 as t from union4 left join dateNewDemo on union4._c0 = dateNewDemo._c0 ").repartition(1).write().csv("file:///home/union5");
          // sparkSession.read().csv("file:///home/union5/union5.csv").registerTempTable("union5");
         //sparkSession.sql("select count(*) from union3").show();
          // sparkSession.read().csv("file:///home/phonesDemo/").registerTempTable("phone");
           //sparkSession.sql("select * from phone").show();
           //sparkSession.sql("select union5.*,phone._c1 as r from union5 left join phone on union5._c0 = phone._c0 ").repartition(1).write().csv("file:///home/union6");
        /*
           sparkSession.read().csv("file:///home/union1/union1.csv").registerTempTable("union1");
           sparkSession.sql("select * from union1").show();
           sparkSession.sql("select count(*) from union1").show();
           sparkSession.sql("select distinct count(*) from union1").show();
        */
        sparkSession.stop();
    }
}
