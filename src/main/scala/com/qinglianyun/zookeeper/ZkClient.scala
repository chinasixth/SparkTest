package com.qinglianyun.zookeeper

import java.util
import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.slf4j.{Logger, LoggerFactory}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:03 2018/11/23
  * @ 
  */
object ZkClient extends Watcher with Serializable {
  val logger: Logger = LoggerFactory.getLogger(ZkClient.getClass)

  private val zk_connect = "192.168.116.11:2181,192.168.116.12:2181,192.168.116.13:2181"
  private var zk: ZooKeeper = null

  // 进程锁的使用
  private val latch = new CountDownLatch(1)

  /*
   * 连接的状态，这个东西必须有，至于为什么，自己研究
   * */
  override def process(watchedEvent: WatchedEvent): Unit = {
    if (latch.getCount > 0 && watchedEvent.getState == Event.KeeperState.SyncConnected) {
      latch.countDown()
    }
  }

  /*
  * 判断path是否存在
  * */
  def exists(zk: ZooKeeper, path: String): Boolean = {
    val stat: Stat = zk.exists(path, false)
    if (stat == null) {
      logger.info(s"$path is not exists......")
      return false
    }
    logger.info(s"$path is exists......")
    true
  }

  /*
  * 创建path
  * */
  def create(zk: ZooKeeper, path: String, value: String): Unit = {
    val pathValue = isValidate(path)
    // 判断path是否存在，如果存在不需要创建
    if (exists(zk, pathValue)) {
      println(s"$pathValue is exists......")
      logger.info(s"$pathValue is exists......")
    } else {
      zk.create(pathValue, value.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      println(s"创建 ${pathValue} 成功......")
      logger.info(s"创建 ${pathValue} 成功......")
    }
  }

  /*
  * 获取某个节点下的所有子节点
  * */
  def getChildren(zk: ZooKeeper, path: String): util.List[String] = {
    val pathValue = isValidate(path)
    zk.getChildren(pathValue, true)
  }

  /*
  * 判断输入的路径是否合法
  * 当path为null或者empty时，直接报错返回
  * 当path == 1时，判断是不是"/"，如果是，直接返回结果继续操作；如果不是，用"/"和path拼接起来，然后返回结果继续操作
  * 当path > 1，判断是否以"/"开头，不是则加上；判断是否以"/"结尾，是则删除，然后继续操作
  * */
  def isValidate(path: String): String = {
    var pathValue = path
    if (path.isEmpty || path == null) {
      println("path is null")
      logger.error("path is null")
    } else if (path.length == 1) {
      if (!path.startsWith("/")) {
        logger.info(s"$path is not start with character, then append prefix by myself")
        return "/" + path
      }
    } else {
      if (!path.startsWith("/")) {
        pathValue = "/" + path
        logger.info(s"$path is not start with character, then append prefix by myself")
      } else {
        pathValue = path
      }
      if (path.endsWith("/")) {
        logger.info(s"$path is end with '/' character, then split '/' by myself")
        pathValue = pathValue.substring(0, pathValue.length - 1)
      }
    }
    pathValue
  }

  /*
  * 获取数据
  * */
  def getData(path: String): Array[Byte] = {
    val pathValue = isValidate(path)
    zk.getData(pathValue, false, zk.exists(pathValue, false))
  }


  def delete(zk: ZooKeeper, path: String): Unit = {
    val pathValue = isValidate(path)
    if (exists(zk, pathValue)) {
      zk.delete(pathValue, -1)
      logger.info(s"$pathValue was delete......")
      println(s"$pathValue was delete......")
    }
  }

  def main(args: Array[String]): Unit = {

    zk = new ZooKeeper(zk_connect, 2000, ZkClient)

    val path = "zkdata"
    val value = "hello zookeeper"

//        create(zk, path, value)
    //    val list: util.List[String] = getChildren(zk, "brokers/")
    //
    //    // 以下是将Java的集合类型转换成Scala的集合类型，要不然不能使用循环访问
    //    import scala.collection.JavaConversions._
    //    for (i <- list) {
    //      println(i)
    //      val dataBytes = getData("brokers/" + i)
    //      println(dataBytes)
    //    }

    delete(zk, path)
  }

}
