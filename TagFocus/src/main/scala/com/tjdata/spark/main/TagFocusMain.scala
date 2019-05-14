package com.tjdata.spark.main

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileStatus, Path}
import java.text.SimpleDateFormat
import scala.math.min
import java.util.Date
import java.util.Calendar
import java.util.Locale
import org.slf4j.LoggerFactory
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

/**
  * @author zhangshu
  * 2019年4月11日 上午10:14:34
  * @version 1.0
  */
object TagFocusMain {
  private val logger = LoggerFactory.getLogger(TagFocusMain.getClass())

  def main(args: Array[String]): Unit = {
    try {
      val conf = new SparkConf().setAppName("TagFocusMain").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)
      val hdfs = org.apache.hadoop.fs.FileSystem.newInstance(sc.hadoopConfiguration)
      var fileStatus : Array[FileStatus] = Array[FileStatus]()
      val cal = Calendar.getInstance(Locale.CHINA)
      cal.setFirstDayOfWeek(Calendar.MONDAY)
      if (cal.get(Calendar.DATE) > 5) { cal.add(Calendar.MONTH, -1) } else { cal.add(Calendar.MONTH, -2) }
      val defaultargs = Array("20190412", "F:/test_data/dx_input/tjdata_tag/", "814,815", "201902")
  //    val defaultargs = Array("20190412", "/user/vendorszjsb/tjdata_tag/", "*", new SimpleDateFormat("yyyyMM").format(cal.getTime))
      for (i <- 0 until min(args.length, defaultargs.length)) {
        defaultargs(i) = args(i)
      }
      val hdfsDir = defaultargs(1)
      val pushedDay = new SimpleDateFormat("yyyyMMddHHmmss").parse(defaultargs(0) + new SimpleDateFormat("HHmmss").format(new Date()))
      val periodData = new SimpleDateFormat("yyyy-MM-dd").format(pushedDay)
      val periodFile = new SimpleDateFormat("yyyyMMdd").format(pushedDay)
      val objCls = Class.forName("com.tjdata.spark.handler.e", true, com.tjdata.spark.tools.ClassLoaderConfig.getInstance(hdfsDir + "/lib/"))
      val config = objCls.newInstance()
      val provStr = objCls.getDeclaredMethod("d", classOf[String]).invoke(config, hdfsDir + "/conf/tag_prov.txt").toString
      val cityStr = objCls.getDeclaredMethod("d", classOf[String]).invoke(config, hdfsDir + "/conf/tag_city.txt").toString
      val channel = objCls.getDeclaredMethod("d", classOf[String]).invoke(config, hdfsDir + "/conf/tag_channel.txt").toString.split("\\r\\n", -1).toList.map { _.split("\\~", -1) }.filter { f => f.length == 5 &&  "1".equals(f(4))}.map { case Array(p1, p2, p3, p4, p5) => (p2, p4) }
      val industry = objCls.getDeclaredMethod("d", classOf[String]).invoke(config, hdfsDir + "/conf/tag_switch.txt").toString.split("\\r\\n", -1).toList.map { _.split("\\~", -1) }.filter { f => f.length == 10 &&  "1".equals(f(2))}.map { case Array(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) => (p2, p4) }.toMap
      var imeiPath = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "input_data_imei_root_dir", "/user/vendorszjsb/tjdata_tag/").toString.replaceFirst("\\$\\{}", "{" + defaultargs(2) + "}").replaceFirst("\\$\\{}", defaultargs(3))
      fileStatus = hdfs.globStatus(new Path(imeiPath))
      if (fileStatus == null || fileStatus.length == 0) {
        val imeiCal = Calendar.getInstance(Locale.CHINA)
        imeiCal.setTime(new SimpleDateFormat("yyyyMM").parse(defaultargs(3)))
        imeiCal.add(Calendar.MONTH, -1)
        imeiPath = imeiPath.substring(0, imeiPath.length - 6) + new SimpleDateFormat("yyyyMM").format(imeiCal.getTime)
      }
      val urlPath = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "input_data_url_root_dir", "/user/vendorszjsb/tjdata_tag/").toString
      val dpiCode = objCls.getDeclaredMethod("a", classOf[String], classOf[String], classOf[String]).invoke(config, hdfsDir + "/sys.properties", "channel_code_dpi", "01").toString
      val dpiMap = HashMap[String,ListBuffer[(String, String)]]()
      for (i <- 0 until channel.length) {
        val nums = channel(i)._2.toInt
        val period = Calendar.getInstance(Locale.CHINA)
        period.setTime(pushedDay)
        if (dpiCode == channel(i)._1) {
          for (k <- 0 until nums) {
            val date = new SimpleDateFormat("yyyyMMdd").format(period.getTime)
            fileStatus = hdfs.globStatus(new Path(urlPath + date))
            if (fileStatus != null && fileStatus.length > 0) {
              val inList = ListBuffer[(String, String)]()
              for (i <- industry) {
                fileStatus = hdfs.globStatus(new Path(urlPath + date + "/" + i._1))
                if (fileStatus != null && fileStatus.length > 0) {
                  inList.+=(i)
                }
              }
              if (inList.size > 0) {
                dpiMap.put(date, inList)
              }
            }
            period.add(Calendar.DATE, -1)
          }
        }
      }
      if (dpiMap.size > 0) {
        DpiTag.execute(hdfsDir, imeiPath, sc, hdfs, periodData, periodFile, provStr, cityStr, dpiMap)
      }
      sc.stop()
    } catch {
      case e: Exception =>
        if (e.getMessage().contains("Failed to submit application")) {
          for (t <- e.getStackTrace) {
            logger.error("error = " + t.toString)
          }
          System.exit(1);
        }
    }
  }
}
