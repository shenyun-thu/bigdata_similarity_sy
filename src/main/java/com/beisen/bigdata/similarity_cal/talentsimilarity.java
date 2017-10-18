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

    private static ArrayList<person> p = new ArrayList<>();
    private static double average_score_limit;//平均分最低限制
    private static double max_limit = 1.0;//各个维度上对应的差值最大限度
    private static double similarity_limit = 90.0;//相似度限制

    private static boolean IS_ONLINE = true;

    public static void main(String[] args) throws Exception {
        SparkConf conf1 = new SparkConf();
        conf1.setAppName("talent_similarity");
        JavaSparkContext jsc = new JavaSparkContext(conf1);
        Scan scan = new Scan();
        Configuration conf = SparkUtil.buildHbaseConfigForTable(readTableNamePre + ".ASSESSMENT_ECCOMPETENCEUSERRESULTINFO",scan);
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
                Connection conn = HbaseUtil.getHbaseConnection(IS_ONLINE);
                BufferedMutator mutator = HbaseUtil.getMutator(conn,"beisendw:talentSimilarity");
                try{
                   while (f.hasNext()) {
                        count = 0;
                        count_p_solve = 0;
                        Tuple2<String, Iterable<String>> s = f.next();
                        boolean is_percent = false;
                        for (String temp_s : s._2) {
                            person temp = new person();
                            temp.id = s._1;
                            String[] str = temp_s.split(",");
                            temp.num = str.length - 1;
                            if(temp.num < 10) break;
                            temp.user_id = str[0];
                            for(int i = 1; i <= temp.num; i++){
                                temp.scores[i] = Double.parseDouble(str[i]);
                                if(temp.scores[i] > 10.0) is_percent = true;
                                temp.average += temp.scores[i];
                            }
                            temp.average = temp.average / temp.num;
                            p.add(temp);
                        }
                        if (is_percent) {
                            average_score_limit = 70.0;
                        } else {
                            average_score_limit = 7.0;
                        }

                        //init
                        for (int i = 0,len = p.size();i < len;i++) {
                            if(p.get(i).average > average_score_limit){
                                p.remove(i);
                                --len;
                                --i;
                            }
                        }

                        //  cal_similarity();
                        double max = 0;
                        for (int i = 0; i < p.size(); i++) {
                            for (int j = i + 1; j < p.size(); j++) {
                                double ans = 0;
                                boolean can_cal = true;
                                if (p.get(i).user_id.equals(p.get(i).user_id)) {
                                    can_cal = false;
                                }
                                for (int k = 1; k <= p.get(i).num; k++) {
                                    if ((p.get(i).scores[k] - p.get(i).scores[k]) > max) {
                                        max = p.get(i).scores[k] - p.get(i).scores[k];
                                        if (max > max_limit) {
                                            can_cal = false;
                                            break;
                                        }
                                    }
                                }
                                if (can_cal) {
                                    for (int k = 1; k <= p.get(i).num; k++) {
                                        ans += (p.get(i).scores[k] - p.get(i).scores[k]) * (p.get(i).scores[k] - p.get(i).scores[k]);
                                    }
                                    ans = Math.sqrt(ans);
                                    double similarity_percent = 1 / (1 + 0.02 * ans) * 100;
                                    if (similarity_percent > similarity_limit) {
                                        int temp = Integer.parseInt(p.get(i).id.substring(0, 6));
                                        temp = temp % 256;
                                        String temp_key = String.valueOf(temp);

                                        while (temp_key.length() != 3) {
                                            temp_key = "0" + temp_key;
                                        }

                                        String row_key = temp_key + "_" + p.get(i).id + p.get(i).user_id + "_" + p.get(i).user_id;
                                        Put put = new Put(Bytes.toBytes(row_key));
                                        put.addColumn("fmy".getBytes(),"similarity".getBytes(),(similarity_percent + "%").getBytes());
                                        mutator.mutate(put);

                                        String row_key1 = temp_key + "_" + p.get(i).id + p.get(i).user_id + "_" + p.get(i).user_id;
                                        Put put1 = new Put(Bytes.toBytes(row_key1));
                                        put1.addColumn("fmy".getBytes(),"similarity".getBytes(),(similarity_percent + "%").getBytes());
                                        mutator.mutate(put1);
                                        
                                    }
                                }
                            }
                        }
                    }
                    HbaseUtil.flushMutator(conn,mutator);
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
