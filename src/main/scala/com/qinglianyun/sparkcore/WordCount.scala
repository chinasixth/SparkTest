package com.qinglianyun.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Company: qinglianyun
  * @ Author ：liuhao
  * @ Date   ：Created in 12:54 2018/11/20
  * @ 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
    // 在集群模式下，不用指定
//      .setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val lines = sc.textFile("src/main/data/wordcount.txt")
    val lines = sc.textFile(args(0))
    val words = lines.flatMap(_.split(" "))

    val tuples = words.map(x => (x, 1))
    val result = tuples.reduceByKey(_ + _)

//    println(result.collect.toBuffer)
    // 存储到指定的目录，可以是hdfs
    result.saveAsTextFile(args(1))

    sc.stop()

  }
}
