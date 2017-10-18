package com.beisen.bigdata.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liubaolong on 2016/12/22.
  */
object SparkUtil {

  /**
    * 构建SparkContext
    * @return
    */
  def buildSparkContext(appName:String): SparkContext = {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop")
    val conf = new SparkConf
    conf.setAppName(appName)
    conf.set("spark.executor.heartbeatInterval","30s")
    val sc = new SparkContext(conf)
    sc
  }
  /**
    * 转换Hbase Scan为字符串
    *
    * @param scan
    * @return
    */
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  /**
    * 构建Hbase 配置文件
    *
    * @param hbaseTableName Hbase 表
    */
  def buildHbaseConfigForTable(hbaseTableName: String,family :String, columnNames:Array[String]): Configuration = {
    //设置Hbase的配置文件
    val conf = HBaseConfiguration.create()

    if (columnNames != null && columnNames.length>0) {
      val scan: Scan = new Scan
      scan.addFamily(Bytes.toBytes(family))
      for (column <- columnNames) {
        scan.addColumn(Bytes.toBytes(family), column.getBytes)
      }
      conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    }

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "tjhadoop00,tjhadoop01,tjhadoop02")
//    conf.set("hbase.zookeeper.quorum", "hdfs00,hdfs01,hdfs02")
    conf.set("hbase.client.keyvalue.maxsize","524288000");//
    conf.set("hbase.rpc.timeout","6000000") //RPC超时时间
    conf.set("hbase.client.scanner.timeout.period","6000000")
    conf.set("hbase.client.operation.timeout","120000000")//RPC超时时间
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)

    conf
  }

  def buildHbaseConfigForTable(hbaseTableName: String, scan: Scan): Configuration = {
    //设置Hbase的配置文件
    val conf = HBaseConfiguration.create()
    if (scan != null) {
      conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    }

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "tjhadoop00,tjhadoop01,tjhadoop02")
    //    conf.set("hbase.zookeeper.quorum", "hdfs00,hdfs01,hdfs02")
    conf.set("hbase.client.keyvalue.maxsize","524288000");//
    conf.set("hbase.rpc.timeout","6000000") //RPC超时时间
    conf.set("hbase.client.scanner.timeout.period","6000000")
    conf.set("hbase.client.operation.timeout","120000000")//RPC超时时间
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)

    conf
  }

  /**
    * 构建Hbase 配置文件
    * @param hbaseTableName
    * @param scan
    * @param isOnLine
    *     是否线上Hbase
    * @return
    */
  def buildHbaseConfig(hbaseTableName: String, scan: Scan, isOnLine : Boolean): Configuration = {
    //设置Hbase的配置文件
    val conf = HBaseConfiguration.create()
    if (scan != null) {
      conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    }

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    if(isOnLine) {
      conf.set("hbase.zookeeper.quorum", "tjhadoop00,tjhadoop01,tjhadoop02")
    }else {
      conf.set("hbase.zookeeper.quorum", "hdfs00,hdfs01,hdfs02")
    }
    conf.set("hbase.client.keyvalue.maxsize","524288000");//
    conf.set("hbase.rpc.timeout","6000000") //RPC超时时间
    conf.set("hbase.client.scanner.timeout.period","6000000")
    conf.set("hbase.client.operation.timeout","120000000")//RPC超时时间
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)

    conf
  }

  def getResultRdd(sc: SparkContext, conf: Configuration): RDD[Result] = {
    sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(_._2)
  }


}
