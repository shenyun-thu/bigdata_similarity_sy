package com.beisen.bigdata.similarity_cal;


import java.util.*;
import java.util.ArrayList;
import com.beisen.bigdata.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.beisen.bigdata.util.SparkUtil;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class talentsimilarity {
    private static final String readTableNamePre = "BEISENTALENTDW";
    private static final String family = "0";
    private static final byte[] familyBytes = family.getBytes();
    private static final Logger logger = Logger.getLogger(talentsimilarity.class);

    private static int N = 10000000;
    private static int count_p_solve = 0;  //统计
    private static int count = 0; //记录每一个key值对应下的测试人员数量
    private static int BATCHSIZE = 1000;

    private static person[] p = new person[N];//用于记录所有人
    private static person[] p_solve = new person[N];//用于记录对于每个key值对应的符合要求的人

    private static double average_score_limit;//平均分最低限制
    private static double max_limit = 1.0;//各个维度上对应的差值最大限度
    private static double similarity_limit = 90.0;//相似度限制

    private static boolean ISONLINE = true;

    public static void main(String[] args) throws Exception {
        SparkConf conf1 = new SparkConf();
        conf1.set("spark.kryoserializer.buffer.max", "512");
        //conf1.setMaster("local[*]");
        conf1.setAppName("talent_similarity");
        JavaSparkContext jsc = new JavaSparkContext(conf1);
        String[] columnList = new String[]{
                "TENANTID", "TESTID", "BEISENUSERID",
                "D_EC_27_1", "D_EC_10_2", "D_EC_13_2", "D_EC_9", "D_EC_14",
                "D_EC_1_3", "D_COMPETENCYBASIC_62", "D_COMPETENCYBASIC_54", "D_EC_15_2", "D_EC_2",
                "D_COMPETENCYBASIC_6", "D_COMPETENCYBASIC_25", "D_EC_31_3", "D_EC_31_2", "D_COMPETENCYBASIC_58",
                "D_COMPETENCYBASIC_19", "D_EC_7_1", "D_EC_23_2", "D_COMPETENCYBASIC_130", "D_COMPETENCYBASIC_23",
                "D_EC_21_3", "D_COMPETENCY_24", "D_EC_30_3", "D_EC_35_3", "D_EC_16",
                "D_COMPETENCYBASIC_28", "D_EC_7", "D_COMPETENCY_19", "D_EC_23", "D_EC_3_1",
                "D_COMPETENCY_34", "D_EC_2_3", "D_COMPETENCYBASIC_40", "D_COMPETENCYBASIC_42", "D_EC_14_3",
                "D_COMPETENCYBASIC_9", "D_COMPETENCY_9", "D_COMPETENCYBASIC_63", "D_EC_22_1", "D_COMPETENCY_16",
                "D_EC_35_1", "D_EC_33_3", "D_COMPETENCY_30", "D_COMPETENCYBASIC_33", "D_EC_11_2",
                "D_EC_35", "D_COMPETENCY_100", "D_EC_32", "D_COMPETENCYBASIC_14", "D_COMPETENCYBASIC_61",
                "D_COMPETENCYBASIC_3", "D_COMPETENCY_23", "D_EC_13_1", "D_EC_4", "D_EC_4_3",
                "D_EC_29_2", "D_EC_28_1", "D_EC_16_2", "D_COMPETENCY_25", "D_COMPETENCY_5",
                "D_COMPETENCYBASIC_10", "D_EC_3_3", "D_COMPETENCYBASIC_47", "D_COMPETENCY_12", "D_COMPETENCYBASIC_48",
                "D_COMPETENCY_11", "D_EC_26_2", "D_EC_27", "D_EC_35_2", "D_EC_18_1",
                "D_EC_2_1", "D_COMPETENCY_6", "D_EC_25_1", "D_EC_25_2", "D_COMPETENCY_14",
                "D_COMPETENCY_29", "D_EC_10", "D_EC_3", "D_EC_26_3", "D_COMPETENCYBASIC_24", "D_COMPETENCY_20",
                "D_EC_27_2", "D_EC_9_2", "D_EC_29_1", "D_COMPETENCYBASIC_16", "D_EC_19_3", "D_EC_20", "D_COMPETENCYBASIC_56",
                "D_EC_30_1", "D_EC_25_3", "D_EC_8_2", "D_EC_32_2", "D_EC_36_3",
                "D_EC_1", "D_EC_12", "D_EC_8_3", "D_EC_5", "D_COMPETENCYBASIC_50",
                "D_ECB_SD", "D_EC_20_1", "D_EC_11_3", "D_EC_9_1", "D_COMPETENCYBASIC_4",
                "D_EC_12_3", "D_EC_5_2", "D_EC_31_1", "D_COMPETENCYBASIC_2", "D_EC_17",
                "D_EC_33_2", "D_EC_31", "D_COMPETENCYBASIC_41", "D_EC_12_2", "D_EC_32_3",
                "D_COMPETENCYBASIC_49", "D_EC_11_1", "D_COMPETENCYBASIC_44", "D_EC_16_1",
                "D_EC_14_2", "D_EC_20_2", "D_COMPETENCYBASIC_29", "D_EC_19_2", "D_EC_8",
                "D_COMPETENCYBASIC_37", "D_EC_4_1", "D_EC_24_3", "D_COMPETENCYBASIC_26",
                "D_EC_17_3", "D_EC_29", "D_COMPETENCYBASIC_30", "D_COMPETENCYBASIC_131", "D_COMPETENCYBASIC_132",
                "D_EC_21", "D_COMPETENCY_32", "D_EC_4_2", "D_EC_6_3", "D_COMPETENCY_27", "D_EC_10_1", "D_COMPETENCY_21",
                "D_EC_14_1", "D_COMPETENCY_28", "D_COMPETENCY_33", "D_EC_34_3", "D_COMPETENCY_10", "D_EC_30", "D_EC_23_1",
                "D_EC_8_1", "D_COMPETENCYBASIC_7", "D_EC_13", "D_EC_25", "D_EC_18", "D_EC_34", "D_EC_19", "D_COMPETENCYBASIC_36",
                "D_COMPETENCYBASIC_21", "D_EC_18_3", "D_EC_12_1", "D_EC_29_3", "D_EC_22", "D_EC_19_1", "D_COMPETENCYBASIC_46",
                "D_COMPETENCY_18", "D_COMPETENCYBASIC_64", "D_EC_33_1", "D_COMPETENCYBASIC_52", "D_EC_16_3", "D_EC_6", "D_EC_24",
                "D_EC_9_3", "D_COMPETENCYBASIC_53", "D_COMPETENCYBASIC_5", "D_COMPETENCYBASIC_66", "D_EC_21_2", "D_EC_28_3",
                "D_COMPETENCYBASIC_18", "D_COMPETENCYBASIC_11", "D_COMPETENCYBASIC_72", "D_COMPETENCYBASIC_51", "D_EC_33",
                "D_COMPETENCYBASIC_43", "D_EC_24_2", "D_COMPETENCY_31", "D_COMPETENCYBASIC_59", "D_COMPETENCYBASIC_55", "D_COMPETENCYBASIC_32",
                "D_COMPETENCY_26", "D_EC_26", "D_EC_1_1", "D_EC_6_2", "D_EC_15", "D_COMPETENCYBASIC_75", "D_EC_27_3", "D_EC_26_1", "D_EC_34_2",
                "D_COMPETENCY_17", "D_COMPETENCYBASIC_38", "D_EC_36", "D_EC_2_2", "D_COMPETENCYBASIC_45", "D_COMPETENCYBASIC_20", "D_COMPETENCYBASIC_13",
                "D_EC_3_2", "D_COMPETENCY_1", "D_EC_11", "D_EC_SD", "D_COMPETENCY_8", "D_COMPETENCYBASIC_65", "D_EC_7_3", "D_COMPETENCYBASIC_22", "D_COMPETENCY_3",
                "D_COMPETENCYBASIC_74", "D_EC_17_1", "D_EC_28_2", "D_EC_36_2", "D_EC_13_3", "D_EC_15_1", "D_EC_32_1", "D_COMPETENCYBASIC_39", "D_EC_7_2", "D_COMPETENCY_4",
                "D_EC_5_1", "D_COMPETENCYBASIC_15", "D_EC_22_2", "D_EC_21_1", "D_COMPETENCYBASIC_34", "D_EC_15_3", "D_EC_28", "D_EC_6_1", "D_EC_30_2", "D_EC_10_3", "D_EC_22_3",
                "D_COMPETENCY_7", "D_COMPETENCY_22", "D_COMPETENCY_15", "D_EC_34_1", "D_COMPETENCY_13", "D_EC_5_3", "D_EC_1_2", "D_COMPETENCY_2", "D_EC_18_2", "D_EC_24_1",
                "D_COMPETENCYBASIC_57", "D_EC_17_2", "D_COMPETENCYBASIC_1", "D_COMPETENCYBASIC_17", "D_EC_23_3", "D_EC_36_1", "D_EC_20_3",
        };
        Configuration conf = SparkUtil.buildHbaseConfigForTable(readTableNamePre + ".ASSESSMENT_ECCOMPETENCEUSERRESULTINFO", family, columnList);

        JavaPairRDD<ImmutableBytesWritable, Result> testRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, String> talentRdd = testRdd.filter(new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
            @Override
            public Boolean call(Tuple2<ImmutableBytesWritable, Result> f) throws Exception {
                Result result = f._2;
                String tenantId = new String(result.getValue(familyBytes, "TENANTID".getBytes()));
                if (tenantId != null) {
                    return new Boolean(true);

                }
                return new Boolean(false);
            }
        }).mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
                Result res = t._2;
                Cell[] cells = res.rawCells();
                String tenantId = Bytes.toString(res.getValue(familyBytes, "TENANTID".getBytes()));
                String testId = Bytes.toString(res.getValue(familyBytes, "TESTID".getBytes()));
                String userId = Bytes.toString(res.getValue(familyBytes, "BEISENUSERID".getBytes()));
                while (userId.length() != 9) {
                    userId = userId + "*";
                }
                String value = userId;
                for (int i = 0; i < cells.length; i++) {
                    String temp = Bytes.toString(CellUtil.cloneQualifier(cells[i]));
                    // logger.info(temp);
                    if (temp.startsWith("D_")) {
                        value = value + "," + Bytes.toString(res.getValue(familyBytes, temp.getBytes()));
                    }
                }
                String key = tenantId + "_" + testId + "_";
                return new Tuple2<>(key, value);
            }
        });

        
        talentRdd.groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Iterable<String>>> f) throws Exception {
                Connection conn = HbaseUtil.getHbaseConnection(ISONLINE);
                
                try{
                    int count = 0;
                    ArrayList<Put> putList = new ArrayList();
                    while (f.hasNext()) {
                        count = 0;
                        count_p_solve = 0;
                        Tuple2<String, Iterable<String>> s = f.next();
                        double average_temp = 0.0;
                        for (String temp_s : s._2) {
                            p[count] = new person();
                            p[count].id = s._1;
                            String[] temp = temp_s.split(",");
                            p[count].num = temp.length - 1;
                            if(p[count].num == 0) break;
                            p[count].user_id = temp[0];
                            for (int i = 1; i <= p[count].num; i++) {
                                p[count].scores[i] = Double.parseDouble(temp[i]);
                                p[count].average += p[count].scores[i];
                            }
                            p[count].average = p[count].average / p[count].num;
                            if (p[count].average > average_temp) average_temp = p[count].average;
                            count++;
                        }
                        if (average_temp > 10.0) {
                            average_score_limit = 70.0;
                        } else {
                            average_score_limit = 7.0;
                        }

                        //init
                        count_p_solve = 0;
                        for (int i = 0; i < count; i++) {
                            if (p[i].average > average_score_limit) {
                                p_solve[count_p_solve] = new person();
                                p_solve[count_p_solve] = p[i];
                                count_p_solve++;
                            }
                        }

                        //  cal_similarity();
                        double max = 0;
                        for (int i = 0; i < count_p_solve; i++) {
                            for (int j = i + 1; j < count_p_solve; j++) {
                                double ans = 0;
                                boolean can_cal = true;
                                if (p_solve[i].user_id.equals(p_solve[j].user_id)) {
                                    can_cal = false;
                                }
                                for (int k = 1; k <= p_solve[i].num; k++) {
                                    if ((p_solve[i].scores[k] - p_solve[j].scores[k]) > max) {
                                        max = p_solve[i].scores[k] - p_solve[j].scores[k];
                                        if (max > max_limit) {
                                            can_cal = false;
                                            break;
                                        }
                                    }
                                }
                                if (can_cal) {
                                    for (int k = 1; k <= p_solve[i].num; k++) {
                                        ans += (p_solve[i].scores[k] - p_solve[j].scores[k]) * (p_solve[i].scores[k] - p_solve[j].scores[k]);
                                    }
                                    ans = Math.sqrt(ans);
                                    double similarity_percent = 1 / (1 + 0.02 * ans) * 100;
                                    if (similarity_percent > similarity_limit) {
                                        int temp = Integer.parseInt(p_solve[i].id.substring(0, 6));
                                        temp = temp % 256;
                                        String temp_key = String.valueOf(temp);

                                        while (temp_key.length() != 3) {
                                            temp_key = "0" + temp_key;
                                        }

                                        String rowkey = temp_key + "_" + p_solve[i].id + p_solve[i].user_id + "_" + p_solve[j].user_id;
                                        Put p = new Put(Bytes.toBytes(rowkey));
                                        p.add(Bytes.toBytes("fmy"), Bytes.toBytes("similarity"), Bytes.toBytes(similarity_percent + "%"));
                                        putList.add(p);
                                        
                                        String rowkey1 = temp_key + "_" + p_solve[i].id + p_solve[j].user_id + "_" + p_solve[i].user_id;
                                        Put p1 = new Put(Bytes.toBytes(rowkey1));
                                        p1.add(Bytes.toBytes("fmy"), Bytes.toBytes("similarity"), Bytes.toBytes(similarity_percent + "%"));
                                        putList.add(p1);
                                        
                                        count = count + 2;
                                        
                                        if(count >= BATCHSIZE){
                                            HbaseUtil.putHbase("beisendw:talentSimilarity",putList,ISONLINE);
                                            count = 0;
                                            putList.clear();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if(count > 0){
                        HbaseUtil.putHbase("beisendw:talentSimilarity",putList,ISONLINE);
                    }
                }catch(Exception e){
                        e.printStackTrace();
                }finally {
                    if(conn != null && conn.isClosed()){
                        conn.close();
                    }
                }
            }
        });
    }
}
