package com.mysql;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resources;

@Component
public class test1106 {
    @Autowired
    private static DriverManagerDataSource dataSource;
    @Autowired
    private static JdbcTemplate jdbcTemplate;
    public static void main(String[] args){
         // System.out.println(dataSource.getUrl());
        ApplicationContext context = new ClassPathXmlApplicationContext("application.xml");
        DriverManagerDataSource dataSource= (DriverManagerDataSource) context.getBean("dataSource");
        System.out.println(dataSource.getUrl());

       //System.out.println( new test1106().test().getUrl());

    }

}
