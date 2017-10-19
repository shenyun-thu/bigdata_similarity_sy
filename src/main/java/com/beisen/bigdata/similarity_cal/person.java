package com.beisen.bigdata.similarity_cal;

public class person {
    public String id;//tenantId + testId
    public String user_id;
    public int num  = 0;//维度数
    public double[] scores = new double[500];//从1到num
    public double average = 0.0;
}
