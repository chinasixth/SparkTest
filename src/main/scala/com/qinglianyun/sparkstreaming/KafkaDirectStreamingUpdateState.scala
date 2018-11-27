package com.qinglianyun.sparkstreaming

import com.qinglianyun.utils.ZkUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 20:30 2018/11/26
  * @ desc: 使用direct方式读取kafka中的数据，然后对历史所有的数据进行WordCount
  *
  * 步骤：
  * 1.模板代码
  * 2.接收参数并处理参数
  * 3.创建kafkaParams
  * 4.定义接收kafka数据的样式
  * 5.获取fromOffset
  * 6.读取kafka中的数据
  * 7.保存untilOffset
  * 8.业务逻辑
  *
  * 注意：在使用updateStateByKey时，需要设置检查点
  */
object KafkaDirectStreamingUpdateState {
  private val logger = LoggerFactory.getLogger(KafkaDirectStreamingUpdateState.getClass)

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

    // 设置检查点
    ssc.checkpoint("c://wc-20181127-0")

    val topic = "test"
    val groupId = "groupTest"
    val zkQuorum = "node01:2181,node02:2181,node03:2181"
    val brokerList = "node01:9092,node02:9092,node03:9092"
    val bootstrapServers = "node01:9092,node02:9092,node03:9092"

    val kafkaParams = Map[String, String](
      //      "topic" -> topic,
      "group.id" -> groupId,
      "zookeeper.connect" -> zkQuorum,
      "metadata.broker.list" -> brokerList,
      "bootstrap.servers" -> bootstrapServers,
      "auto.offset.reset" -> "smallest" // largest
    )

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    val fromOffsets: Map[TopicAndPartition, Long] = ZkUtil.getOffset(topic)

    // 注意：必须加上泛型，如果不加泛型将会报错
    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream
      [String,
        String,
        StringDecoder,
        StringDecoder,
        (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    var offsetRanges = Array[OffsetRange]()
    message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      val offsets = ArrayBuffer[String]()
      for (o <- offsetRanges) {
        println(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
        offsets += s"${o.topic},${o.partition},${o.untilOffset}"
      }
      ZkUtil.setOffset(offsets.toArray)
      rdd.foreach(x => println(100))
      println(s"count: ${rdd.count()}")
    }

    val summed: DStream[(String, Int)] = message.map(_._2).flatMap(_.split(" ")).map((_, 1)).updateStateByKey(func,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    summed.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /*
  * 这是向updateStateByKey算子中传的函数
  * 算子中就一个迭代器
  * 迭代器中有三个值：
  * 第一个是：元祖中的每一个单词
  * 第二个是：当前批次中相同单词出现的次数，如Seq(1,1,1)
  * 第三个是：Option代表上一批次相同单词累加的结果，有可能有值，也可能没有值，所以使用Option来封装
  * */
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      // 这里使用的是getOrElse，不能使用get，因为没有值的时候使用get将会返回None，无法参加计算要报错
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

}
