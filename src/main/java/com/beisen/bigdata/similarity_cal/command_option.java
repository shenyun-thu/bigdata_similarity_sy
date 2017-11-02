package com.beisen.bigdata.similarity_cal;

import org.apache.commons.cli.*;
import scala.tools.cmd.gen.AnyVals;

public class command_option {
    public static void main(String[] args) {
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

        String[] ten = tenantIds.split(",");
        for(int i=0;i<ten.length;i++){
            System.out.print(ten[i] + " ");
        }
        System.out.println();
        String[] ass = assessments.split(",");
        for(int i=0;i<ass.length;i++){
            System.out.print(ass[i].split("_")[1]+" ");
        }
        System.out.println();
    }
}
