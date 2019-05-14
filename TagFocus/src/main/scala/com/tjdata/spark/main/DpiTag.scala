package com.tjdata.spark.main

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.HashMap
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.HadoopRDD
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import com.hadoop.mapreduce.LzoTextInputFormat
import com.tjdata.spark.tools.RDDMultipleTextOutputFormat
import scala.collection.immutable.List
import org.apache.hadoop.conf.Configuration

/**
  * @author zhangshu
  * 2019年3月22日 上午10:14:34
  * @version 1.0
  */
object DpiTag {
  def execute(hdfsDir: String, imeiPath: String, sc: SparkContext, hdfs: FileSystem, periodData: String, periodFile: String, provStr: String, cityStr: String, dpiMap: HashMap[String, ListBuffer[(String, String)]]): Unit = {
    val objCls = Class.forName("com.tjdata.spark.handler.e", true, com.tjdata.spark.tools.ClassLoaderConfig.getInstance(hdfsDir + "/lib/"))
    val config = objCls.newInstance()
    val channelCode = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "channel_code_dpi", "01").toString
    val channelDir = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "channel_name_dpi", "dpi").toString
    val prov = provStr.split("\\r\\n", -1).toList.map { _.split("\\~", -1) }.filter { _.length == 6 }.map { case Array(p1, p2, p3, p4, p5, p6) => (p2, p3) }
    val city = cityStr.split("\\r\\n", -1).toList.map { _.split("\\~", -1) }.filter { _.length == 5 }.map { case Array(p1, p2, p3, p4, p5) => (p4, p5) }
    val dateStr = dpiMap.keys.mkString("{", ",", "}")
    val industryMap = dpiMap.values.flatMap { l => l }.toMap
    val industryStr = industryMap.keys.mkString("{", ",", "}")
    val indBroadcast = sc.broadcast(industryMap).value
    val provBroadcast = sc.broadcast(prov.toMap).value
    val cityBroadcast = sc.broadcast(city.toMap).value
    val configuration = new Configuration()
    configuration.set("mapred.output.compress", "true")
    configuration.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
    configuration.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzopCodec")
    configuration.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")
    val imeiFileRDD = sc.newAPIHadoopFile(imeiPath, classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text], configuration)
      .filter { line => line._2 != null }
      .map(l => {
        val sb = new StringBuffer("")
        val cs = l._2.toString.toCharArray
        for (c <- cs) {
          if (c == '\0') {
            sb.append("|")
          } else {
            sb.append(String.valueOf(c))
          }
        }
        sb.toString.split("\\|", -1)
      }).filter(f => f.length == 2 && f(0) != "" && f(1) != "").map(l => (l(0), l(1)))
    val logPath = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "input_data_url_root_dir", "/user/vendorszjsb/tjdata_tag/").toString + "/" + dateStr + "/" + industryStr
    val fileRDD = sc.hadoopFile[LongWritable, Text, TextInputFormat](logPath)
    val hadoopRDD = fileRDD.asInstanceOf[HadoopRDD[LongWritable, Text]]
    val funionmfRDD = hadoopRDD.mapPartitionsWithInputSplit((inputSplit:InputSplit, iterator:Iterator[(LongWritable, Text)]) =>{
      val file = inputSplit.asInstanceOf[FileSplit]; val path = file.getPath.toString().split("\\/", -1);
      iterator.filter { f => f._2 != null }.map(l => (path, l._2.toString.split ("\\|", -1)))
    }).filter { f => f._2.length == 14 && f._2(0) != "" && f._2(6) != "" && f._2(6).length == 14 }
      .map(l => (l._1, l._2(0), l._2(1), l._2(6).substring(8, 10), l._2(7), l._2(8), l._2(12)))
      .map(l => {
        val provId = l._1(l._1.length - 1).split("\\-", -1)(1); val iCode = l._1(l._1.length - 2)
        var result = List[((String, String, String, String, String, String, String, String, String, String), (String, Int))]()
        val clazz = com.tjdata.spark.tools.ClassLoaderConfig.getInstance(hdfsDir + "/lib/").loadClass("com.tjdata.spark.handler.e")
        val iList = clazz.getDeclaredMethod("a", classOf[String], classOf[Map[String, String]], classOf[String], classOf[String]).invoke(getConf(clazz), hdfsDir, indBroadcast.filter(_._1 == iCode), l._6, l._5).asInstanceOf[List[(String, String, String, String, String, String)]]
        iList.foreach(f => {
          result = ((f._1, f._2, f._3, l._2, f._4, f._5, l._4, provBroadcast.getOrElse(provId, ""), l._7, f._6), (l._3, 1)) :: result
        })
        result
      }).filter(f => f.length != 0)
    val resRDD = funionmfRDD.flatMap {l => l}.reduceByKey((x, y) => (x._1, x._2 + y._2)).map(l => (l._2._1, (l._1, l._2))).leftOuterJoin(imeiFileRDD).map(l => (l._2._1._1, (l._2._2.getOrElse(l._2._1._2._1), l._2._1._2._2)))
                        .map(l => ((l._1._1, periodData + "|" + l._1._2 + "|" + l._1._4 + "|" + l._2._1 + "|" + l._1._5 + "|" + l._1._6 + "|" + l._1._7 + "|" + l._1._8 + "|" + l._1._9, l._1._10),(l._2._2))).reduceByKey(_+_)
                        .map(l => { if (l._1._3 != "") (l._1._1 + "/" + channelDir, l._1._1 + "|" + l._1._2 + "|" + l._2 + "|" + l._1._3) else (l._1._1 + "/" + channelDir, l._1._1 + "|" + l._1._2 + "|" + l._2) })
    if (!resRDD.isEmpty()) {
      val cacheRDD = resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val outPath = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "output_data_root_dir", "/user/vendorszjsb/industry_label/").toString
      val outPathTmp = outPath + "/prma_output_tmp/" + periodFile + "/"
      val outPathNor = outPath + "/prma_output/" + periodFile + "/"
      val files = hdfs.globStatus(new Path(outPathNor + industryStr + "/" + channelDir))
      if (files != null && files.length > 0) { for (f <- files) if (hdfs.exists(new Path(f.getPath.toString))) hdfs.delete(new Path(f.getPath.toString), true) }
      if (hdfs.exists(new Path(outPathTmp))) { hdfs.delete(new Path(outPathTmp), true) }
      cacheRDD.saveAsHadoopFile(outPathTmp, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat[_,_]], classOf[GzipCodec])
      val filesTmp = hdfs.globStatus(new Path(outPathTmp + industryStr + "/" + channelDir))
      if (filesTmp != null && filesTmp.length > 0) {
        val size = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "file_size", "128").toString.toInt
        for (f <- filesTmp) {
          val iPath = f.getPath.toString
          val res = objCls.getDeclaredMethod("c", classOf[String]).invoke(config, iPath + "/*").toString.toDouble
          val partition = if (res / size > 1) Math.ceil(res / size).toInt else 1
          val key = iPath.substring(iPath.substring(0, iPath.lastIndexOf("/")).lastIndexOf("/") + 1)
          cacheRDD.filter(f => key.equals(f._1)).coalesce(partition, true).saveAsHadoopFile(outPathNor, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat[_,_]], classOf[GzipCodec])
        }
      }
      if (hdfs.exists(new Path(outPathTmp))) { hdfs.delete(new Path(outPathTmp), true) }
      cacheRDD.unpersist(true)
      val historyFiles = hdfs.globStatus(new Path(outPath + "/prma_output/*"))
      val days = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "file_days_of_preservation", "7").toString.toInt
      if (historyFiles != null && historyFiles.length > days) {
        var dateList = List[String]()
        for (f <- historyFiles) dateList = dateList.+: (f.getPath.toString)
        for (i <- 0 until (historyFiles.length - days)) {
          val path = dateList.reverse(i)
          if (hdfs.exists(new Path(path))) hdfs.delete(new Path(path), true)
        }
      }
    }
  }

  private var loaderConf: Any = null

  private def getConf(cls: Class[_]): Any = {
    if (null == loaderConf) {
      this.synchronized{
        if (null == loaderConf) {
          loaderConf = cls.newInstance()
        }
      }
    }
    return loaderConf
  }
}