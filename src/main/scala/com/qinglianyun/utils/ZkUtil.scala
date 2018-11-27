package com.qinglianyun.utils

import java.util
import java.util.concurrent.CountDownLatch
import kafka.common.TopicAndPartition
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper._

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:30 2018/11/23
  * @
  */

object ZkUtil extends Watcher with Serializable{

  protected var countDownLatch: CountDownLatch = new CountDownLatch(1)
  override def process(event: WatchedEvent): Unit = {
    if (event.getState eq Event.KeeperState.SyncConnected) {
      countDownLatch.countDown
    }
  }

  val zk = new ZooKeeper("192.168.116.11:2181,192.168.116.11:2181,192.168.116.11:2181", 5000, ZkUtil)
  val parentPath = "/qinglianyun"
  //默认partition的数量
  val initPartitions = 1
  //默认offset的值
  val initOffset = 0+""
  //这里虽然没有显示的调用，但是会被执行
  if (zk.exists(parentPath, false) == null){
    zk.create(parentPath, "0".getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  /**
    * 通过topic获取partition以及相应的offset
    *
    * @param topic
    * @return
    */
  def getOffset(topic:String): Map[TopicAndPartition, Long] ={
    val zkPath = parentPath + "/" + topic
    val map = scala.collection.mutable.Map[TopicAndPartition, Long]()

    /**
      * 如果topic节点不存在，那么就创建
      * 并且直接初始化partition节点，而且初始化值都为 initOffset
      */
    if (zk.exists(zkPath, false) == null){
      zk.create(zkPath, "0".getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      for(i <- 0 to initPartitions - 1){
        zk.create(zkPath + "/" + i, initOffset.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      }
    }
    /**
      * 返回offset
      */
    val children = zk.getChildren(zkPath, false)
    val iterator: util.Iterator[String] = children.iterator()
    while (iterator.hasNext){
      val child: String = iterator.next()
      val offset = new String(zk.getData(zkPath +"/"+ child, false, null))
      val tp = new TopicAndPartition(topic, child.toInt)
      map += (tp -> offset.toLong)

    }
    map.toMap
  }

  /**
    * 设置偏移量
    *
    * @param offsets "topic,partition,offset"
    */
  def setOffset(offsets : Array[String]): Unit ={
    offsets.foreach(off =>{
      val splits: Array[String] = off.split(",")
      val partitionPath = parentPath + "/" + splits(0) + "/" + splits(1)
      if (zk.exists(partitionPath, false) == null){
        //默认值是0
        zk.create(partitionPath, splits(2).getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      }else{
        zk.setData(partitionPath, splits(2).getBytes, -1)
      }
    })
  }
}
