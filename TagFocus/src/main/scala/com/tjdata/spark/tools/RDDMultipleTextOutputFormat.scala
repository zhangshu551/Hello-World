package com.tjdata.spark.tools

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.{FileSystem}
import org.apache.hadoop.mapred.JobConf

/**
 * @author zhangshu
 * 2019年3月22日 下午5:14:30
 * @version 1.0
 */
class RDDMultipleTextOutputFormat[K, V] extends MultipleTextOutputFormat[K, V] {
  override def generateActualKey(key: K, value: V): K = NullWritable.get().asInstanceOf[K]
  
  override def generateFileNameForKeyValue(key: K, value: V, name: String): String = s"$key/$name"
  
  override def checkOutputSpecs(f: FileSystem, job: JobConf): Unit = {}
}