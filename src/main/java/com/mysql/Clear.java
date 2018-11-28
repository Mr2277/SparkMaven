package com.mysql;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;


public class Clear {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        //要连接的数据库URL
        String url = "jdbc:mysql://10.168.7.245:3306/business_data_db";
        //连接的数据库时使用的用户名
        String username = "root";
        //连接的数据库时使用的密码
        String password = "zhirong123";
        File f = new File("/home/e12.txt");
       // File f=new File("D:\\t.txt");
        OutputStream out = null; // 准备好一个输出的对象

        out = new FileOutputStream(f); // 通过对象多态性，进行实例化


//1.加载驱动
//DriverManager.registerDriver(new com.mysql.jdbc.Driver());不推荐使用这种方式来加载驱动
        Class.forName("com.mysql.jdbc.Driver");//推荐使用这种方式来加载驱动

        //2.获取与数据库的链接
        Connection conn = DriverManager.getConnection(url, username, password);
          /*
        //3.获取用于向数据库发送sql语句的statement
        Statement st = conn.createStatement();
             String limit="4000000";
             String num="1";
        ResultSet rs=st.getResultSet();
             while (true) {

                 //String sql = "select id from enterprise where id='u' limit 50000000 offset 49999999   ";
                 String sql=String.format("select id,reg_no from enterprise limit %s , %s",limit,num);
                 //4.向数据库发sql,并获取代表结果集的resultset
                 System.out.println(sql);
               rs = st.executeQuery(sql);

                 if(!rs.next()){
                     break;
                 }
//5.取出结果集的数据
                 while (rs.next()) {

                     String str = rs.getObject("id") + "," + rs.getObject("reg_no").toString().replace(",","，") + "\t\n";
                     byte b[] = str.getBytes(); // 只能输出byte数组，所以将字符串变为byte数组
                     out.write(b); // 将内容输出，
                 }
                 limit= String.valueOf(Integer.parseInt(limit)+500);
                 num=String.valueOf(Integer.parseInt(num)+500);
             }
        rs.close();

        st.close();
        */
        String limit="43999999";
        String num="4000000";
        //String limit="0";
        //String num="3999999";
        String sql=String.format("select id,econ_kind,category from enterprise limit %s , %s",limit,num);
        //rs = ps.executeQuery();

        PreparedStatement preparedStatement=conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        preparedStatement.setFetchSize(500);
        preparedStatement.setFetchDirection(ResultSet.FETCH_REVERSE);
        ResultSet rs=preparedStatement.executeQuery();
        long count=0;
        int temp=1;
        while (rs.next()) {
            String str="";
            String econ_kind="";
            String category="";
            if(rs.getObject("econ_kind")!=null){
               econ_kind=rs.getString("econ_kind").replace(",","，");
            }
            if(rs.getObject("category")!=null){
                category=rs.getString("category");
            }
            str=rs.getObject("id")+","+econ_kind+","+category+"\r\n";
            byte b[] = str.getBytes(); // 只能输出byte数组，所以将字符串变为byte数组
            out.write(b); // 将内容输出，
            count++;
            if(count==500){
            System.out.println(count*temp);
            temp++;
            count=0;
            }
        }
        rs.close();
        out.close();
        conn.close();

        //6.关闭链接，释放资源
    }
}
