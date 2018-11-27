package com.qinglianyun.sparkstreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 15:17 2018/11/22
  * @ 
  */
object KafkaSourceStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("KafkaSourcesStreaming")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))

//    ssc.checkpoint("c:/cp_20181122")

    val Array(zkQuorum, groupId, topics, numThreads) = args

    // 指定每个topic及相应的处理线程数，需要转换成map类型，因为人家要的是map类型
    val topicMap: Map[String, Int] = topics.split(",").map((_, numThreads.toInt)).toMap

    /*
    * 参数说明：
    * 1.指定ssc
    * 2.指定zookeeper
    * 3.指定消费组id
    * 4.指定topic，以及相应处理的线程数
    * 5.指定接收的数据存储方式
    * */
    val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap, StorageLevel.MEMORY_AND_DISK)

    val valueStream: DStream[String] = kafkaStream.map(_._2)

    val pairs: DStream[(String, Int)] = valueStream.flatMap(_.split(" ")).map((_, 1))

    val result: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
