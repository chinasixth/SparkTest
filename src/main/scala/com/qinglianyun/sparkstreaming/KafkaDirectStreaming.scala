package com.qinglianyun.sparkstreaming

import com.qinglianyun.utils.ZkUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 20:11 2018/11/22
  * @ desc：使用direct方式读取kafka中的数据，并将offset保存到zookeeper
  * 思路：第一次启动程序的时候，根据指定的topic，去zookeeper中取offset，根据offset到kafka中取数据；
  *      然而，第一次启动时，zookeeper中根据就没有保存topic的offset信息，所以，需要自己先初始化offset值；
  *      因为topic中的每一个partition都有一个offset，即在zookeeper上需要维护topic的每一个partition的offset，
  *      所以在zookeeper上初始化每个partition的offset的时候，要知道kafka中topic的分区数，如果多了会报错，如果少了，在某一个分区中就会无法读取数据或者是从头读取。
  *      读取到数据以后，需要更新zookeeper中offset的值，这就需要按照分区遍历读取到的数据，将每个分区中的offset取出来，然后存储到zookeeper中。
  *
  * 开发步骤：
  *   1.模板代码
  *   2.接收参数，并处理参数
  *   3.定义kafka参数
  *   4.定义接收kafka数据的的形式
  *   5.读取zookeeper中的offset
  *   6.读取kafka数据
  *   7.保存kafka中的offset
  *   8.业务逻辑
  */
object KafkaDirectStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("conf.setMaxSpoutPending", "true")
      .set("auto.commit.interval.ms", "1000")
      .set("spark.streaming.kafka.maxRatePerPartition", "3000")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // 接收参数
//    val Array(zkQuorum, groupId, topics) = args
    val topics = "test"

    val fromOffsets: Map[TopicAndPartition, Long] = ZkUtil.getOffset(topics)

    // 将kafka的消息transform成(topic_name, message)这种形式
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    // 定义kafka参数
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "192.168.116.11:2181,192.168.116.12:2181,192.168.116.13:2181",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest",
      "metadata.broker.list" -> "192.168.116.11:9092,192.168.116.12:9092,192.168.116.13:9092",
//      "topic" -> topics,设置这个东西是无效的。
      "group.id" -> "groupTest"
    )

    //    val messages: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc, kafkaParams, fromOffsets, messageHandler)
    // 使用direct方式读取kafka的数据
    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    // 保存每个批次的offset --来自官网
    var offsetRanges = Array[OffsetRange]()
    message.transform((rdd: RDD[(String, String)]) => {
      // HasOffsetRanges只有在message的第一个方法中调用时才会成功，原因是在调用任何shuffle或重分区的算子之后，RDD分区和Kafka分区之间的一对一映射不会保留
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).foreachRDD(rdd => {

      // offset 管理
      val offsets = ArrayBuffer[String]()
      for (o <- offsetRanges) {
        println(s"${o.topic}   ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
        offsets += s"${o.topic},${o.partition},${o.untilOffset}"
      }

      // TODO offset保存的时间点   根据需求而定
      // 保存offsets
      ZkUtil.setOffset(offsets.toArray)

      // TODO 这里写要实现的业务逻辑
      rdd.foreach(println)
      println("######业务逻辑######")

      println(rdd.count())
    })
    // TODO 业务逻辑，如下为WordCount
    val count: DStream[(String, Int)] = message.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    count.print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
