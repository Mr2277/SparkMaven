package com.mysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Hu1105 {
    public static void main(String [] args) throws ClassNotFoundException, SQLException {
        String url = "jdbc:mysql://192.168.194.4:3306/F_CL_ENTERPRISE_DB?useUnicode=true&characterEncoding=UTF-8";
        //连接的数据库时使用的用户名
        String username = "root";
        //连接的数据库时使用的密码
        String password = "zhirong123";
        Class.forName("com.mysql.jdbc.Driver");//推荐使用这种方式来加载驱动
        //2.获取与数据库的链接
        Connection conn = DriverManager.getConnection(url, username, password);
        String sql="select SHORT_NAME from hu1105 where SHORT_NAME is not NULL ";
        PreparedStatement preparedStatement=conn.prepareStatement(sql);
        ResultSet rs=preparedStatement.executeQuery();
        int count=0;
        List<String> list=new ArrayList<String>();
        StringBuilder stringBuilder=new StringBuilder();

        while(rs.next()){
            count++;
            String SHORT_NAME=rs.getString("SHORT_NAME");
            char ch[]=SHORT_NAME.toCharArray();
            for(int i=0;i<ch.length;i++){
                stringBuilder.append('%');
                stringBuilder.append(ch[i]);
                //stringBuilder.append('%');
            }
            stringBuilder.append("%");
            //System.out.println(stringBuilder.toString());
            ////String H_HNAME_FINAL="%"+H_HNAME+"%";
            list.add(stringBuilder.toString());
            stringBuilder.delete(0,stringBuilder.length());


        }
        Statement statement=conn.createStatement();
        /*
        String sql3="select FULL_NAME from hu1105 where SHORT_NAME like '%阿%拉%善%农%村%商%业%银%行%'";
       preparedStatement=conn.prepareStatement(sql3);
        rs=preparedStatement.executeQuery();
        while (rs.next()){
           System.out.println( rs.getString("FULL_NAME"));


        }
         */

        for(String s:list) {
           // System.out.println(s);
            String sql5=String.format("select FULL_NAME from hu1105 where SHORT_NAME like '%s'",s);
           // System.out.println(sql5);
            //String sql1 = "select FULL_NAME from hu1105 where SHORT_NAME like ?";
            preparedStatement = conn.prepareStatement(sql5);
           // preparedStatement.setString(1, s);
            rs = preparedStatement.executeQuery();
            while(rs.next()) {
                String FULL_NAME = rs.getString("FULL_NAME");
                System.out.println(FULL_NAME + "          " + s);
            }
            // System.out.println(rs.next());
        }
            /*
            while(rs.next()){
                String FULL_NAME=rs.getString("FULL_NAME");
                System.out.println(FULL_NAME+"          "+s);
               // String sql2=String.format("insert into hu1105_1 values('%s','%s')",FULL_NAME,s);
               // System.out.println(sql2);
                //statement.executeUpdate(sql2);
            }
        }
         */
        rs.close();
        conn.close();

    }
}
