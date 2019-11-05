package com.clear;

public class JiJin {
    public static void main(String[] args){
        double num=5000;
        double lilv=1;
        double save=0;
        for(int i=0;i<12*10;i++){
            num=num*(1+0.02);
            num=num+5000;
            lilv=lilv*(1+0.01);
            save+=5000;
        }
        System.out.println(num);
        System.out.println(lilv);
        System.out.println(save);
    }
}
