package com.mysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Clear1008 {
    public static void main(String [] args) throws ClassNotFoundException, SQLException {
        //要连接的数据库URL
        String url = "jdbc:mysql://192.168.194.4:3306/F_CL_ENTERPRISE_DB?characterEncoding=UTF-8";
        //连接的数据库时使用的用户名
        String username = "root";
        //连接的数据库时使用的密码
        String password = "zhirong123";
        Class.forName("com.mysql.jdbc.Driver");//推荐使用这种方式来加载驱动
        //2.获取与数据库的链接
        Connection conn = DriverManager.getConnection(url, username, password);
        String sql="select H_HNAME from t0925_c9 where  H_HIDTYPE=0";
        PreparedStatement preparedStatement=conn.prepareStatement(sql);
        ResultSet rs=preparedStatement.executeQuery();
        int count=0;
        List<String> list=new ArrayList<String>();
        while(rs.next()){
            count++;
            String H_HNAME=rs.getString("H_HNAME");
            String H_HNAME_FINAL="%"+H_HNAME+"%";
            list.add(H_HNAME_FINAL);
        }
        System.out.println(count);
        Statement statement=conn.createStatement();
        String H_HNAME="";
        String E_ENAME="";
        String E_EID="";
        for(String s:list){
            System.out.println(s);
            H_HNAME=s.substring(1,s.length()-1);
            String sql1="select * from enterprise_t where E_ENAME like ?";
            preparedStatement=conn.prepareStatement(sql1);
            preparedStatement.setString(1,s);
            rs=preparedStatement.executeQuery();
            while(rs.next()){
                E_ENAME=rs.getString("E_ENAME");
                E_EID=rs.getString("E_EID");
                System.out.println(H_HNAME+"   "+E_ENAME+"    "+E_EID);
                String sql2=String.format("insert into t0925_c10 values('%s','%s','%s')",E_ENAME,E_EID,H_HNAME);
                System.out.println(sql2);
                statement.executeUpdate(sql2);
            }
            /*
            Statement stmt=conn.createStatement();
            ResultSet selectRes = stmt.executeQuery(sql1);
            System.out.println(selectRes.getRow());
            */

        }
        rs.close();
        conn.close();
    }
}
