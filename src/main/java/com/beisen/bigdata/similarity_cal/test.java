package com.beisen.bigdata.similarity_cal;

import java.util.ArrayList;

public class test {
    public static void main(String[] args){
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        for (int i = 0,len = list.size();i < len;i++) {
            if(list.get(i) == 3){
                list.remove(i);
                --len;
                --i;
            }
        }
        
        for(int i = 0;i < list.size();i++){
            System.out.println(list.get(i));
        }
    }
}

//    String[] columnList = new String[]{
//            "TENANTID", "TESTID", "BEISENUSERID",
//            "D_EC_27_1", "D_EC_10_2", "D_EC_13_2", "D_EC_9", "D_EC_14",
//            "D_EC_1_3", "D_COMPETENCYBASIC_62", "D_COMPETENCYBASIC_54", "D_EC_15_2", "D_EC_2",
//            "D_COMPETENCYBASIC_6", "D_COMPETENCYBASIC_25", "D_EC_31_3", "D_EC_31_2", "D_COMPETENCYBASIC_58",
//            "D_COMPETENCYBASIC_19", "D_EC_7_1", "D_EC_23_2", "D_COMPETENCYBASIC_130", "D_COMPETENCYBASIC_23",
//            "D_EC_21_3", "D_COMPETENCY_24", "D_EC_30_3", "D_EC_35_3", "D_EC_16",
//            "D_COMPETENCYBASIC_28", "D_EC_7", "D_COMPETENCY_19", "D_EC_23", "D_EC_3_1",
//            "D_COMPETENCY_34", "D_EC_2_3", "D_COMPETENCYBASIC_40", "D_COMPETENCYBASIC_42", "D_EC_14_3",
//            "D_COMPETENCYBASIC_9", "D_COMPETENCY_9", "D_COMPETENCYBASIC_63", "D_EC_22_1", "D_COMPETENCY_16",
//            "D_EC_35_1", "D_EC_33_3", "D_COMPETENCY_30", "D_COMPETENCYBASIC_33", "D_EC_11_2",
//            "D_EC_35", "D_COMPETENCY_100", "D_EC_32", "D_COMPETENCYBASIC_14", "D_COMPETENCYBASIC_61",
//            "D_COMPETENCYBASIC_3", "D_COMPETENCY_23", "D_EC_13_1", "D_EC_4", "D_EC_4_3",
//            "D_EC_29_2", "D_EC_28_1", "D_EC_16_2", "D_COMPETENCY_25", "D_COMPETENCY_5",
//            "D_COMPETENCYBASIC_10", "D_EC_3_3", "D_COMPETENCYBASIC_47", "D_COMPETENCY_12", "D_COMPETENCYBASIC_48",
//            "D_COMPETENCY_11", "D_EC_26_2", "D_EC_27", "D_EC_35_2", "D_EC_18_1",
//            "D_EC_2_1", "D_COMPETENCY_6", "D_EC_25_1", "D_EC_25_2", "D_COMPETENCY_14",
//            "D_COMPETENCY_29", "D_EC_10", "D_EC_3", "D_EC_26_3", "D_COMPETENCYBASIC_24", "D_COMPETENCY_20",
//            "D_EC_27_2", "D_EC_9_2", "D_EC_29_1", "D_COMPETENCYBASIC_16", "D_EC_19_3", "D_EC_20", "D_COMPETENCYBASIC_56",
//            "D_EC_30_1", "D_EC_25_3", "D_EC_8_2", "D_EC_32_2", "D_EC_36_3",
//            "D_EC_1", "D_EC_12", "D_EC_8_3", "D_EC_5", "D_COMPETENCYBASIC_50",
//            "D_ECB_SD", "D_EC_20_1", "D_EC_11_3", "D_EC_9_1", "D_COMPETENCYBASIC_4",
//            "D_EC_12_3", "D_EC_5_2", "D_EC_31_1", "D_COMPETENCYBASIC_2", "D_EC_17",
//            "D_EC_33_2", "D_EC_31", "D_COMPETENCYBASIC_41", "D_EC_12_2", "D_EC_32_3",
//            "D_COMPETENCYBASIC_49", "D_EC_11_1", "D_COMPETENCYBASIC_44", "D_EC_16_1",
//            "D_EC_14_2", "D_EC_20_2", "D_COMPETENCYBASIC_29", "D_EC_19_2", "D_EC_8",
//            "D_COMPETENCYBASIC_37", "D_EC_4_1", "D_EC_24_3", "D_COMPETENCYBASIC_26",
//            "D_EC_17_3", "D_EC_29", "D_COMPETENCYBASIC_30", "D_COMPETENCYBASIC_131", "D_COMPETENCYBASIC_132",
//            "D_EC_21", "D_COMPETENCY_32", "D_EC_4_2", "D_EC_6_3", "D_COMPETENCY_27", "D_EC_10_1", "D_COMPETENCY_21",
//            "D_EC_14_1", "D_COMPETENCY_28", "D_COMPETENCY_33", "D_EC_34_3", "D_COMPETENCY_10", "D_EC_30", "D_EC_23_1",
//            "D_EC_8_1", "D_COMPETENCYBASIC_7", "D_EC_13", "D_EC_25", "D_EC_18", "D_EC_34", "D_EC_19", "D_COMPETENCYBASIC_36",
//            "D_COMPETENCYBASIC_21", "D_EC_18_3", "D_EC_12_1", "D_EC_29_3", "D_EC_22", "D_EC_19_1", "D_COMPETENCYBASIC_46",
//            "D_COMPETENCY_18", "D_COMPETENCYBASIC_64", "D_EC_33_1", "D_COMPETENCYBASIC_52", "D_EC_16_3", "D_EC_6", "D_EC_24",
//            "D_EC_9_3", "D_COMPETENCYBASIC_53", "D_COMPETENCYBASIC_5", "D_COMPETENCYBASIC_66", "D_EC_21_2", "D_EC_28_3",
//            "D_COMPETENCYBASIC_18", "D_COMPETENCYBASIC_11", "D_COMPETENCYBASIC_72", "D_COMPETENCYBASIC_51", "D_EC_33",
//            "D_COMPETENCYBASIC_43", "D_EC_24_2", "D_COMPETENCY_31", "D_COMPETENCYBASIC_59", "D_COMPETENCYBASIC_55", "D_COMPETENCYBASIC_32",
//            "D_COMPETENCY_26", "D_EC_26", "D_EC_1_1", "D_EC_6_2", "D_EC_15", "D_COMPETENCYBASIC_75", "D_EC_27_3", "D_EC_26_1", "D_EC_34_2",
//            "D_COMPETENCY_17", "D_COMPETENCYBASIC_38", "D_EC_36", "D_EC_2_2", "D_COMPETENCYBASIC_45", "D_COMPETENCYBASIC_20", "D_COMPETENCYBASIC_13",
//            "D_EC_3_2", "D_COMPETENCY_1", "D_EC_11", "D_EC_SD", "D_COMPETENCY_8", "D_COMPETENCYBASIC_65", "D_EC_7_3", "D_COMPETENCYBASIC_22", "D_COMPETENCY_3",
//            "D_COMPETENCYBASIC_74", "D_EC_17_1", "D_EC_28_2", "D_EC_36_2", "D_EC_13_3", "D_EC_15_1", "D_EC_32_1", "D_COMPETENCYBASIC_39", "D_EC_7_2", "D_COMPETENCY_4",
//            "D_EC_5_1", "D_COMPETENCYBASIC_15", "D_EC_22_2", "D_EC_21_1", "D_COMPETENCYBASIC_34", "D_EC_15_3", "D_EC_28", "D_EC_6_1", "D_EC_30_2", "D_EC_10_3", "D_EC_22_3",
//            "D_COMPETENCY_7", "D_COMPETENCY_22", "D_COMPETENCY_15", "D_EC_34_1", "D_COMPETENCY_13", "D_EC_5_3", "D_EC_1_2", "D_COMPETENCY_2", "D_EC_18_2", "D_EC_24_1",
//            "D_COMPETENCYBASIC_57", "D_EC_17_2", "D_COMPETENCYBASIC_1", "D_COMPETENCYBASIC_17", "D_EC_23_3", "D_EC_36_1", "D_EC_20_3",
//    };
//Configuration conf = SparkUtil.buildHbaseConfigForTable(readTableNamePre + ".ASSESSMENT_ECCOMPETENCEUSERRESULTINFO", family, columnList);