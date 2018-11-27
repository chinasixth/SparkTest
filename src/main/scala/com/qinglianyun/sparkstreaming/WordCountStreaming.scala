package com.qinglianyun.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:40 2018/11/22
  * @ 
  */
object WordCountStreaming {
  def main(args: Array[String]): Unit = {

    // 模板代码
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))

    //    还可以这么写模板代码，这也是比较正常的写法
    //    val conf = new SparkConf()
    //      .setAppName(this.getClass.getName)
    //      .setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //    val ssc = new StreamingContext(sc, Seconds(1))

    // 使用ssc创建一个socket流
    // socketTextStream可以传第三个参数，表示数据的存储级别
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.116.11", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val tuples: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = tuples.reduceByKey(_ + _)

    // 默认打印十个，内部调用了foreachRDD，向foreachRDD传入了一个自定义的函数
    result.print()

    // 模板代码
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
