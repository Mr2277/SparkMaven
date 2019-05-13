package com.clear;

import java.util.*;

public class exp {
    public static void main(String[] args){
          String strToMatch="建筑安装业 土木工程建筑业 软件和信息技术服务业 电气机械和器材制造业 计算机、通信和其他电子设备制造业 金属制品业 仪器仪表制造业 专用设备制造业 商务服务业 土木工程建筑业";
          String doma[]=strToMatch.split(" ");
          String [] domacopy=new String[1000];
         // System.out.println(domacopy.length);
          int j=0;
          for(int i=0;i<doma.length;i++){
              if(i==0){
                  domacopy[j]=doma[i];
                  j++;
              }
              else{
                  int k;
                  for(k=0;k<j;k++){
                     if(doma[i].equals(domacopy[k])){
                        break;
                     }
                  }
                  if(k==j){
                      domacopy[k]=doma[i];
                      j++;
                  }
              }
              //domacopy
          }
        StringBuilder builder=new StringBuilder();

        for(int h=0;h<j;h++){
            if(h<j-1) {
                builder.append(domacopy[h]);
                builder.append(" ");
            }
            else{
                builder.append(domacopy[h]);
            }
          }
        System.out.println(builder.toString());
          /*
          Set<String>set =new TreeSet<>();
          for(String s:doma) {
              set.add(s);
          }
          System.out.println(set.size());
        Iterator<String>value=set.iterator();
        StringBuilder builder=new StringBuilder();
        while(value.hasNext()){
            builder.append(value.next());
            builder.append(" ");
        }
        */
         /*
        List<String>list=new ArrayList<>();
        int i;
        for(String s:doma){
            for( i=0;i<list.size();i++){
                 if(s.equals(list.get(i))){
                     break;
                 }
            }
            if(i==list.size()){
                list.add(s);
            }
        }
        System.out.println(list.size());
        StringBuilder builder=new StringBuilder();
        for(String s:list){
            builder.append(s);
            builder.append(" ");
        }
          //List<String> list=new ArrayList<>();
        System.out.println(builder.toString());
        */

    }
}
