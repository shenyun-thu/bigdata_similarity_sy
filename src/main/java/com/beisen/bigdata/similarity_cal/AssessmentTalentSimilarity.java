package com.beisen.bigdata.similarity_cal;

import com.beisen.bigdata.util.HbaseUtil;
import com.beisen.bigdata.util.RowKeyUtil;
import com.beisen.bigdata.util.SparkUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class AssessmentTalentSimilarity {
    
    public static HBaseConfiguration hBaseConfiguration = null;

    public static String tableName = "beisendw:LBL_TalentCenterIdMap";

    static{
        Configuration conf2 = HBaseConfiguration.create();
        conf2.set("hbase.zookeeper.property.clientPort", "2181");
        conf2.set("hbase.zookeeper.quorum", "tjhadoop00,tjhadoop01,tjhadoop02");
        hBaseConfiguration = new HBaseConfiguration(conf2);
    }
    
    public static boolean IS_ONLINE = true;
    public static int BATCH_SIZE = 1000;
    
    public static String getRowKey(String info)throws Exception{
        String[] temp = info.split("_");
        String tenantId = temp[0];
        String userId = temp[1];
        String row_key = RowKeyUtil.getBussinessUniqueFieldRowkey("AU",tenantId,userId);
        
        HTable table = new HTable(hBaseConfiguration,tableName);
        Get g = new Get(Bytes.toBytes(row_key));
        Result r = table.get(g);
        String person_key = new String(r.getValue("fmy".getBytes(),"personkey".getBytes()));
        return person_key;
    }
    
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("AssessmentTalentSimilarity");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String[] columnList = new String[]{"similarity"};
        Configuration conf1 = SparkUtil.buildHbaseConfigForTable("beisendw:talentSimilarity","fmy",columnList);

        JavaPairRDD<ImmutableBytesWritable, Result> testRdd = jsc.newAPIHadoopRDD(conf1, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String,String> talentRdd = testRdd.filter(new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
            @Override
            public Boolean call(Tuple2<ImmutableBytesWritable, Result> f) throws Exception {
                Result result = f._2;
                String similarity = new String(result.getValue("fmy".getBytes(),"similarity".getBytes()));
                if(similarity != null){
                    return new Boolean(true);
                }
                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
                Result res = t._2;
                String row_key = Bytes.toString(res.getRow());
                String[] temp = row_key.split("_");
                String tenantId = temp[1];
                String testId = temp[2];
                String userId = temp[3];
                String similarId = temp[4];
                
                String key =  tenantId + "_" + userId;
                String value = testId + "_" + similarId;
                return new Tuple2<>(key,value);
             }
        });
        
        talentRdd.groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Iterable<String>>> f) throws Exception {
                Connection conn = HbaseUtil.getHbaseConnection(IS_ONLINE);
                try{
                    int count = 0;
                    ArrayList<Put> putList = new ArrayList();
                    while(f.hasNext()){
                        Tuple2<String,Iterable<String>> s = f.next();
                        
                        String row_key = getRowKey(s._1);
                        
                        
                        String talentSimilarityPerson = null;
                        for(String temp_s : s._2){
                            talentSimilarityPerson = temp_s + "||";
                        }
                        
                        Put p = new Put(Bytes.toBytes(row_key));
                        p.add(Bytes.toBytes("fmy"),Bytes.toBytes("talentSimilarityPerson"),Bytes.toBytes(talentSimilarityPerson));
                        putList.add(p);
                        count = count + 1;

                        if(count >= BATCH_SIZE){
                            HbaseUtil.putHbase("beisendw:LBL_TalentCenter_testSY",putList,IS_ONLINE);
                            count = 0;
                            putList.clear();
                        }
                    }
                    if(count > 0){
                        HbaseUtil.putHbase("beisendw:LBL_TalentCenter_testSY",putList,IS_ONLINE);
                    }
                }catch (Exception e){
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
