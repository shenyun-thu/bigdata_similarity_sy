package com.beisen.bigdata.similarity_cal;

import java.text.DecimalFormat;
import java.util.ArrayList;
import com.beisen.bigdata.util.HbaseUtil;
import org.apache.commons.cli.*;
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
import com.beisen.bigdata.util.SparkUtil;
import scala.Tuple2;

public class talentsimilarity {
    
    private static final Logger logger = Logger.getLogger(talentsimilarity.class);
    // BEISENTALENTDW.ASSESSMENT_ECCOMPETENCEUSERRESULTINFO,BEISENTALENTDW.ASSESSMENT_MCCOMPETENCEUSERRESULTINFO,
    // BEISENTALENTDW.ASSESSMENT_ABILITYUSERRESULTINFO,BEISENTALENTDW.ASSESSMENT_RUITUUSERRESULTINFO
    
    private static boolean IS_ONLINE = true;
    
    public static void cal_similarity(String tenantIds,String tableNames,JavaSparkContext jsc){
        String[] str = tenantIds.split(",");
        Scan scan = new Scan();
        Configuration conf = SparkUtil.buildHbaseConfig(tableNames,scan,IS_ONLINE);
        JavaPairRDD<ImmutableBytesWritable, Result> testRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        testRdd.filter(f -> {
            Result result = f._2;
            String tenantId = new String(result.getValue("0".getBytes(),"TENANTID".getBytes()));
            if (tenantIds.equals("*")) {
                return true;
            }
            else{
                for(int i = 0;i< str.length;i++){
                    if(str[i].equals(tenantId)){
                        return true;
                    }
                }
            }
            return false;
        }).mapToPair(m ->{
            Result res = m._2;
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
        }).groupByKey().foreachPartition(f->{
                ArrayList<person> p = new ArrayList<>(); //使用动态数组存储信息
                double average_score_limit = 7.0;//平均分最低限制 可以动态调整
                double max_limit_1 = 0.5;//维度 是 4 到10 的限制
                double max_limit_2 = 3;//维度是 10 到 20 的限制
                double max_limit = 0.0;
                double similarity_limit_1 = 90.0;//维度是4 到10 的相似度限制
                double similarity_limit_2 = 90.0;//维度是10 到20 的限制
                double similarity_limit = 87.0;
                int dim_limit = 4;//维度数量限制
            
                if(tableNames.equals("BEISENTALENTDW.ASSESSMENT_ECCOMPETENCEUSERRESULTINFO")){
                    average_score_limit = 7.0;
                    max_limit_1 = 0.5;
                    max_limit_2 = 3;
                    similarity_limit_1 = 90.0;
                    similarity_limit_2 = 90.0;
                    similarity_limit = 87.0;
                    dim_limit = 4;
                }else if(tableNames.equals("BEISENTALENTDW.ASSESSMENT_MCCOMPETENCEUSERRESULTINFO")){
                    average_score_limit = 6.0;
                    max_limit_1 = 1;
                    max_limit_2 = 3;
                    similarity_limit_1 = 80.0;
                    similarity_limit_2 = 80.0;
                    similarity_limit = 80.0;
                    dim_limit = 4;
                }else if(tableNames.equals("BEISENTALENTDW.ASSESSMENT_ABILITYUSERRESULTINFO")){
                    average_score_limit = 6.0;
                    max_limit_1 = 2.0;
                    max_limit_2 = 2.0;
                    similarity_limit_1 = 83.0;
                    similarity_limit_2 = 80.0;
                    similarity_limit = 80.0;
                    dim_limit = 4;
                }else if(tableNames.equals("BEISENTALENTDW.ASSESSMENT_RUITUUSERRESULTINFO")){
                    average_score_limit = 6.0;
                    max_limit_1 = 0.5;
                    max_limit_2 = 3;
                    similarity_limit_1 = 90.0;
                    similarity_limit_2 = 80.0;
                    similarity_limit = 80.0;
                    dim_limit = 4;
                }
                
                Connection conn = HbaseUtil.getHbaseConnection(IS_ONLINE);
                BufferedMutator mutator = HbaseUtil.getMutator(conn,"beisendw:talentSimilarity_test");
                try{
                    while (f.hasNext()) {
                        p.clear();//对于每一个key值清空p中的存储
                        Tuple2<String, Iterable<String>> s = f.next();
                        boolean is_percent = false;//判断满分是百分还是十分
                        boolean ave_limit = true;
                        for (String temp_s : s._2) {
                            person temp = new person();
                            temp.id = s._1;
                            String[] sco = temp_s.split(",");//获取各项分数
                            temp.num = sco.length - 2;//value值的第一项为userID 最后一项为SN
                            if(temp.num < dim_limit){
                                ave_limit = false;
                                break;//维度小于指定值的不计算
                            }
                            temp.user_id = sco[0];
                            temp.sn = sco[sco.length - 1];
                            for(int i = 1; i <= temp.num; i++){
                                temp.scores[i] = Double.parseDouble(sco[i]);
                                if(temp.scores[i] > 10.0) is_percent = true;
                                temp.average += temp.scores[i];
                            }
                            temp.average = temp.average / temp.num;

                            p.add(temp);
                        }
                        if(!ave_limit)  continue;

                        //这里可以根据数据量和数据分布对平均数进行动态修改
                        if (is_percent) {
                            average_score_limit *= 10;
                            max_limit_1 *= 10;
                            max_limit_2 *= 10;
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
            });
    }
    
    public static void main(final String[] args) throws Exception {
        SparkConf sc = new SparkConf();
        sc.setAppName("talent_similarity");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        
        Options opt = new Options();
        opt.addOption("h", "help", false, "print help for the command.");
        opt.addOption("tid", "tenantIds", true, "talentId array split with \",\" , all for \"*\" ");
        opt.addOption("ass", "assessments", true, "assessment table name split with \"，\", all for \"*\"");
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        String formatstr = "-tid tenantIds(\"*\"|\"tenantId1,tenantId2,.....\") -ass assessments_tables_name(\"*\"|\"table1,table2,.....\") [-h/--help]";
        try {
            cmd = parser.parse(opt, args);
        } catch (Exception e) {
            formatter.printHelp(formatstr, opt);
            System.exit(-1);
        }

        if (cmd.hasOption("h") || !cmd.hasOption("tid") || !cmd.hasOption("ass")) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(formatstr, "", opt, "");
            System.exit(-1);
        }

        String tenantIds = cmd.getOptionValue("tid");
        String assessments = cmd.getOptionValue("ass");
        String[] tables = assessments.split(",");
        
        for(int i = 0;i < tables.length;i++){
            cal_similarity(tenantIds,tables[i],jsc);
        }
    }
}
