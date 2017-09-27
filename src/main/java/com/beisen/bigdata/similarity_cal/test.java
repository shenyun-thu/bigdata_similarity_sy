package com.beisen.bigdata.similarity_cal;

public class test {
    public static void main(String[] args){
        String s = "000_101120_0D2E3952-8E80-4B26-9298-5F64AEFE3CCE_104413487_107400777";
        String[] temp = s.split("_");
        for(int i = 1;i < temp.length;i++){
            System.out.println(temp[i]);
        }
    }
}
