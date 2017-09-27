package com.beisen.bigdata.util;

import com.beisen.bigdata.helper.common.MD5Util;

/**
 * HBase RowKey生成规则
 * Created by liubaolong on 2017/3/24.
 */
public class RowKeyUtil {

    /**
     * 左侧填充字符
     * @param originString
     * @param len
     * @param padChar
     * @return
     */
    public static String leftPad(String originString, int len,char padChar){
        int originStrLen=0;
        if(originString!=null&&!originString.isEmpty()){
            originStrLen=originString.length();
        }
       int padLen=len-originStrLen;
        final char[] buff=new char[padLen];
        for(int i=0;i<padLen;i++){
            buff[i]=padChar;
        }
        return new String(buff).concat(originString);
    }

    /**
     * 获取Job_Recommand_Training 表的RowKey
     *
     * @param tenantId
     * @param jobId
     * @param applicantId
     * @return
     */
    public static String getJob_Recommand_Training_RowKey(String tenantId, String jobId, String applicantId) {

        /**
         * 格式  2位散列值 6位租户ID,9位jobId,9位applicantId
         * rowkey 的散列处理，租户Id 个位和十位 64取模
         * 例如 110061 =>61 按64取模 13 =>13110061
         * rowkey 611100 000000000 000000000
         */

        int num=Integer.valueOf( tenantId.substring(4,6));

        return  leftPad((num%64)+"",2,'0')+leftPad(tenantId,6,'0')+leftPad(jobId,9,'0')+leftPad(applicantId,9,'0');
    }

    /**
     * 获取 job_recommand_model表的RowKey
     * @param tenantId
     * @param jobId
     * @return
     */
    public static String getJob_recommand_model_RowKey(String tenantId, String jobId) {
        /**
         * 格式  2位散列值 6位租户ID,9位jobId
         * rowkey 的散列处理，租户Id 个位和十位 64取模
         * 例如 110061 =>61 按64取模 13 =>13110061
         */
        int num=Integer.valueOf( tenantId.substring(4,6));
        return  leftPad((num%64)+"",2,'0')+leftPad(tenantId,6,'0')+leftPad(jobId,9,'0');
    }


    /**
     * 获取人才库盘点rowKey
     * @param tenantId
     * @param isBest
     * @param currJobCategory
     * @param applicantId
     * @return
     */
    public static String getTalent_TalentStoreDbStatistic_RowKey(String tenantId, String isBest,String currJobCategory,String applicantId) {
        /**
         * 格式  2位散列值 6位租户ID,9位jobId
         * rowkey 的散列处理，租户Id 个位和十位和百位 256 取模
         * 例如 110061 =>61 按64取模 13 =>13110061
         */

        int num =0;
        if(tenantId.length()>=3){
            num=Integer.valueOf( tenantId.substring(tenantId.length()-3));
        }else{
            num=Integer.valueOf(tenantId);
        }
        return  leftPad( Integer.toHexString(num%256)+"",2,'0')+leftPad(tenantId,8,'0')+isBest+ (currJobCategory.isEmpty()?"0000000000000000": MD5Util.getMd5with16bits(currJobCategory)) +leftPad(applicantId,10,'0');
    }

    /***
     * 获取人才中心Map rowKey
     * @param tenantId
     * @param applicantId
     * @return
     */
    public static String getTalentCenterMap_RowKey(String tenantId,String applicantId){

        int num =0;
        if(tenantId.length()>=3){
            num=Integer.valueOf( tenantId.substring(tenantId.length()-3));
        }else{
            num=Integer.valueOf(tenantId);
        }

        return  leftPad(String.valueOf(num%256),3,'0')+leftPad(tenantId,13,'0')+leftPad(applicantId,16,'0');
    }



    public static String getBussinessUniqueFieldRowkey(String talentUniqueType, String tenantId, String businessUnique){
        return MD5Util.getMd5(talentUniqueType+tenantId+businessUnique);
    }

    public static void main(String[] args) {
//        ,email=,mobile=13675815756 ,email=,mobile=15829305251陈凯,email=742737130@qq.com,mobile=18108602682
        //,email=陈琳,email=695654663@qq.com,mobile=15071799954    吴迪,email=584487063@qq.com,mobile=18938635922
        // 陈琳,email=963722590@qq.com,mobile=18611454391tenantId_applicantId=102159355552785
        //赵新新,email=859081527@qq.com,mobile=18852288086tenantId_applicantId=106365194268198
//        System.out.println(getBasicUniqueFieldRowkey("赵新新","859081527@qq.com"));
//        System.out.println(getBasicUniqueFieldRowkey("赵新新","18852288086"));
//        System.out.println( getTalentCenterMap_RowKey("110006", "178842"));//100102,1727614
//        System.out.println( getJob_recommand_model_RowKey("110006","430007720"));
//        System.out.println(getJob_Recommand_Training_RowKey("104802","560022647","563124060"));
//        System.out.println(getTalentCenter_RowKey("赵容郡","8396189@qq.com",null));
//        System.out.println(getJob_recommand_model_RowKey("110006","430007816"));
//        for (int i = 0; i < 64; i++) {
//             System.out.println( (i<10?"0"+i:i)+ "000000"+"000000000"+ "000000000");
//        }

    }
}
