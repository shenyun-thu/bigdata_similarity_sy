package com.beisen.bigdata.similarity_cal;


import java.text.DecimalFormat;
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
    
    private static final Logger logger = Logger.getLogger(talentsimilarity.class);
    
    private static ArrayList<person> p = new ArrayList<>(); //使用动态数组存储信息
    private static double average_score_limit = 6.0;//平均分最低限制 可以动态调整
    private static double max_limit_1 = 1;//维度 是 4 到10 的限制
    private static double max_limit_2 = 3;//维度是 10 到 20 的限制
    private static double max_limit = 0;
    private static double similarity_limit_1 = 80.0;//维度是4 到10 的相似度限制
    private static double similarity_limit_2 = 80.0;//维度是10 到20 的限制
    private static double similarity_limit = 80.0; 
    private static int nums_limit = 4;//维度数量限制
    private static boolean IS_ONLINE = true;

    public static void main(final String[] args) throws Exception {
        
        String[] str = args[0].split(",");
        SparkConf sc = new SparkConf();
        sc.setAppName("talent_similarity");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Scan scan = new Scan();
        Configuration conf = SparkUtil.buildHbaseConfig("BEISENTALENTDW.ASSESSMENT_MCCOMPETENCEUSERRESULTINFO",scan,IS_ONLINE);
        JavaPairRDD<ImmutableBytesWritable, Result> testRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, String> talentRdd = testRdd.filter(new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
            @Override
            public Boolean call(Tuple2<ImmutableBytesWritable, Result> f) throws Exception {
                Result result = f._2;
                String tenantId = new String(result.getValue("0".getBytes(),"TENANTID".getBytes()));
                if (args[0].equals("*")) {
                    return new Boolean(true);
                }
                else{
                    for(int i = 0;i< str.length;i++){
                        if(str[i].equals(tenantId)){
                            return new Boolean(true);
                        }
                    }
                }
                return new Boolean(false);
            }
        }).mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
                Result res = t._2;
                Cell[] cells = res.rawCells();
                String tenantId = Bytes.toString(res.getValue("0".getBytes(), "TENANTID".getBytes()));
                String testId = Bytes.toString(res.getValue("0".getBytes(), "TESTID".getBytes()));
                String userId = Bytes.toString(res.getValue("0".getBytes(), "BEISENUSERID".getBytes()));
                String sn = Bytes.toString(res.getValue("0".getBytes(),"SN".getBytes()));     
                while (userId.length() < 9) {
                    userId = userId + "*";
                }//规定的beiSenUserID正常的长度为9
                
                String value = userId;
                for (int i = 0; i < cells.length; i++) {
                    String temp = Bytes.toString(CellUtil.cloneQualifier(cells[i]));
                    if (temp.startsWith("D_")) {
                        value = value + "," + Bytes.toString(res.getValue("0".getBytes(), temp.getBytes()));
                    }
                }//获取各维度上的分数
                value = value + "," + sn;
                String key = tenantId + "_" + testId + "_";
                return new Tuple2<>(key, value);
            }
        });


        talentRdd.groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Iterable<String>>> f) throws Exception {
                Connection conn = HbaseUtil.getHbaseConnection(IS_ONLINE);
                BufferedMutator mutator = HbaseUtil.getMutator(conn,"beisendw:talentSimilarity_mccompetenceuserresultinfo");
                try{
                   while (f.hasNext()) {
                        p.clear();//对于每一个key值清空p中的存储
                        Tuple2<String, Iterable<String>> s = f.next();
                        boolean is_percent = false;//判断满分是百分还是十分
                        boolean ave_limit = true;
                        for (String temp_s : s._2) {
                            person temp = new person();
                            temp.id = s._1;
                            String[] str = temp_s.split(",");//获取各项分数
                            temp.num = str.length - 2;//value值的第一项为userID 最后一项为SN
                            if(temp.num < nums_limit){
                                ave_limit = false;
                                break;//维度小于指定值的不计算
                            }
                            temp.user_id = str[0];
                            temp.sn = str[str.length - 1];
                            for(int i = 1; i <= temp.num; i++){
                                temp.scores[i] = Double.parseDouble(str[i]);
                                if(temp.scores[i] > 10.0) is_percent = true;
                                temp.average += temp.scores[i];
                            }
                            temp.average = temp.average / temp.num;

                            p.add(temp);
                        }
                        if(!ave_limit)  continue;
//                        
//                        for(int i = 0;i < p.size();i++){
//                            average_score_limit += p.get(i).average;
//                        }
//                        average_score_limit = average_score_limit / p.size();
                       
                        //这里可以根据数据量和数据分布对平均数进行动态修改
                        if (is_percent) {
                            average_score_limit *= 10;
                            max_limit *= 10;
                        } 

                        //对平均分不符合限制的直接从数组里删除
                        for (int i = 0,len = p.size();i < len;i++) {
                            if(p.get(i).average < average_score_limit){
                                p.remove(i);
                                --len;
                                --i;
                            }
                        }
                        
                        if(p.size() < 2) continue;
                        
                        if(p.get(0).num < 10) 
                        {
                            max_limit = max_limit_1;//维度大于4小于10的时候 由于维度数目较少 所以各个维度上取值差异应当也较小
                            similarity_limit = similarity_limit_1;
                        }
                        if(p.get(0).num < 20 && p.get(0).num >= 10) 
                        {
                            max_limit = max_limit_2;//维度大于10小于20的时候 各维度上取值差异可以适当放宽   大于20个维度的时候不考虑各个维度上的差异
                            similarity_limit = similarity_limit_2;
                        }
                        
                        //计算相似度
                        for (int i = 0; i < p.size(); i++) {
                            for (int j = i + 1; j < p.size(); j++) {
                                double ans = 0;
                                boolean can_cal = true;//判断是否符合各个维度上最大差值的要求
                                
                                if (p.get(i).user_id.equals(p.get(j).user_id)) {
                                    can_cal = false;
                                }//同一个人做过同一个测试多次 排除相似计算

                                //计算各维度上的最大差值 对不满足条件的部分跳过计算
                                double max_temp = 0;
                                for (int k = 1; k <= p.get(i).num; k++) {
                                    if (Math.abs(p.get(i).scores[k] - p.get(j).scores[k]) > max_temp) {
                                        max_temp = Math.abs(p.get(i).scores[k] - p.get(j).scores[k]);
                                        
                                        if (max_temp > max_limit && p.get(i).num < 20) {
                                            can_cal = false;
                                            break;
                                        }
                                        
                                    }
                                }
                                
                                if (can_cal) {
                                    
                                    //计算欧氏距离
                                    for (int k = 1; k <= p.get(i).num; k++) {
                                        ans += (p.get(i).scores[k] - p.get(j).scores[k]) * (p.get(i).scores[k] - p.get(j).scores[k]);
                                    }
                                    ans = Math.sqrt(ans);
                                    
                                    double similarity_percent = 1 / (1 + 0.02 * ans) * 100;//大致要求为欧氏距离小于5.5满足相似度大于90%要求
                                    DecimalFormat df = new DecimalFormat("#.00");
                                    if (similarity_percent > similarity_limit) { 
                                        
                                        //存储表分256个区  为方便查找 设计的key值前三位为tenantID模256得到的三位数字
                                        int temp = Integer.parseInt(p.get(i).id.substring(0, 6));
                                        temp = temp % 256;
                                        String temp_key = String.valueOf(temp);
                                        while (temp_key.length() != 3) {
                                            temp_key = "0" + temp_key;
                                        }

                                        //为方便查找 对同一组相似数据存储为两份
                                        String row_key = temp_key + "_" + p.get(i).id + p.get(i).user_id + "_" + p.get(i).sn + "_" + p.get(j).user_id + "_" + p.get(j).sn;
                                        Put put = new Put(Bytes.toBytes(row_key));
                                        put.addColumn("fmy".getBytes(),"similarity".getBytes(),(df.format(similarity_percent) + "%").getBytes());
                                        mutator.mutate(put);

//                                        String row_key1 = temp_key + "_" + p.get(i).id + p.get(j).user_id + "_" + p.get(i).user_id;
//                                        Put put1 = new Put(Bytes.toBytes(row_key1));
//                                        put1.addColumn("fmy".getBytes(),"similarity".getBytes(),(similarity_percent + "%").getBytes());
//                                        mutator.mutate(put1);
                                        
                                    }
                                }
                            }
                        }
                    }
                    HbaseUtil.flushMutator(conn,mutator);//异步写入数据库
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
