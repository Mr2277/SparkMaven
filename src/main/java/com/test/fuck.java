package com.test;

import java.io.*;
import java.sql.*;

public class fuck {
    public static void main(String [] args) throws IOException, ClassNotFoundException, SQLException {
//要连接的数据库URL
        String url = "jdbc:mysql://10.168.7.245:3306/business_data_db";
        //连接的数据库时使用的用户名
        String username = "root";
        //连接的数据库时使用的密码
        String password = "zhirong123";
        //File f = new File("/home/e12.txt");
         File f=new File("D:\\ffffyyyyyyf.txt");
        OutputStream out = null; // 准备好一个输出的对象
        out = new FileOutputStream(f); // 通过对象多态性，进行实例化
        Class.forName("com.mysql.jdbc.Driver");//推荐使用这种方式来加载驱动
        //2.获取与数据库的链接
        Connection conn = DriverManager.getConnection(url, username, password);
        String limit="0";
        String num="10000";
        //String limit="0";
        //String num="3999999";
        String sql=String.format("select a,b,c from t limit %s , %s",limit,num);
        //rs = ps.executeQuery();
        PreparedStatement preparedStatement=conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        preparedStatement.setFetchSize(10000);
        preparedStatement.setFetchDirection(ResultSet.FETCH_REVERSE);
        ResultSet rs=preparedStatement.executeQuery();
        long count=0;
        int temp=1;
        double si=0;
        double guoyou=0;
        double waiguo=0;
        double other =0;
        String last="";
        String current="";
        while (rs.next()) {
            current=(String) rs.getObject("a");
            if("".equals(last)){
                last= (String) rs.getObject("a");
            }
            if(!last.equals(current)){
               String result=last+","+si+","+guoyou+","+waiguo+","+other+"\r\n";
                byte b[] = result.getBytes(); // 只能输出byte数组，所以将字符串变为byte数组
                out.write(b); // 将内容输出
                last="";
                si=0;
                guoyou=0;
                waiguo=0;
                other=0;

            }
                String b=rs.getString("b");
                 if("个体工商户".equals(b) || "自然人".equals(b)){
                     si+=rs.getDouble("c");
                 }
                 if("事业单位".equals(b) || "机关".equals(b) || "社会团体".equals(b)){
                     guoyou+=rs.getDouble("c");
                 }
                 if("企业".equals(b) || "境外自然人".equals(b)){
                     waiguo+=rs.getDouble("c");
                 }
                 if("村委会".equals(b) || b==null){
                     other+=rs.getDouble("c");
                 }
        }
        rs.close();
        out.close();
        conn.close();
    }
}
