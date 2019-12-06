package com.mysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test1206 {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //要连接的数据库URL
        String url = "jdbc:mysql://192.168.194.4:3306/F_CL_ENTERPRISE_DB?characterEncoding=UTF-8";
        //连接的数据库时使用的用户名
        String username = "root";
        //连接的数据库时使用的密码
        String password = "zhirong123";
        Class.forName("com.mysql.jdbc.Driver");//推荐使用这种方式来加载驱动
        //2.获取与数据库的链接
        Connection conn = DriverManager.getConnection(url, username, password);
        String sql="select * from t1206 ";
        PreparedStatement preparedStatement=conn.prepareStatement(sql);
        ResultSet rs=preparedStatement.executeQuery();
        Statement statement=conn.createStatement();

        int count=0;
        while (rs.next()){
            count++;
            double []data = new double[10000];

            String H_ENAME=rs.getString("H_ENAME");
            String SUMSHARE=rs.getString("SUMSHARE");
            String GROUPHNAME=rs.getString("GROUPHNAME");
            String GROUPSHARE=rs.getString("GROUPSHARE");
            String HNAME[]=GROUPHNAME.split("@");
            String SHARE[]=GROUPSHARE.split("@");
            List<String>SHAREList= new ArrayList<>();
            for(int i=0;i<SHARE.length;i++){
                SHAREList.add(SHARE[i]);
            }
            List<String>HNAMEList=new ArrayList<>();
            for(int i=0;i<HNAME.length;i++){
                HNAMEList.add(HNAME[i]);
            }
            /*
            for(int i=0;i<SHARE.length;i++){
                data[i]=Double.parseDouble(SHARE[i]);
            }
            */
            //System.out.println(data.length+"  "+SHARE.length);
            double sum=Double.parseDouble(SUMSHARE);
            StringBuilder DHNAME=new StringBuilder();
            StringBuilder DSHARE=new StringBuilder();

            while(sum>=1.01){
                double min=9999;
                int temp=0;
                for(int i=0;i<SHAREList.size();i++){
                   // System.out.println(SHAREList.get(i));
                    if(Double.parseDouble(SHAREList.get(i))< min){
                        temp=i;
                        min=Double.parseDouble(SHAREList.get(i));
                    }
                }

                //SHAREList.remove(temp);
               // System.out.println(temp+"&&"+min+"&&"+HNAMEList.get(temp));
                DHNAME.append(HNAMEList.get(temp));
                DHNAME.append("@");
                DSHARE.append(min);
                DSHARE.append("@");
                SHAREList.remove(temp);
                HNAMEList.remove(temp);


                sum=sum-min;

            }
            //System.out.println(H_ENAME+"&&"+sum+"&&"+DHNAME.toString()+"&&"+DSHARE.toString());
            String resultHNAME[]=DHNAME.toString().substring(0,DHNAME.toString().length()-1).split("@");
            String resultSHARE[]=DSHARE.toString().substring(0,DSHARE.toString().length()-1).split("@");

            for(int i=0;i<resultHNAME.length;i++){
                String sql2=String.format("insert into t12061 values('%s','%s','%s')",H_ENAME,resultHNAME[i],resultSHARE[i]);
                System.out.println(sql2);
                statement.executeUpdate(sql2);
            }


            //String sql2=String.format("insert into t12061 values('%s','%s','%s','%s','%s','%s')",H_ENAME,SUMSHARE,H_HNAME);

        }
    }
}
