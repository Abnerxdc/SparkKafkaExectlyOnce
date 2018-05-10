package com.abner.spark

import com.abner.dao.KafkaOffsetDao
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{KafkaManager, SparkConf, SparkContext}

/**
  * Created by Admin on 2018/5/10.
  * @author xudacheng
  */
object KafkaSpark {
  /**
    * 程序启动后先执行最下面的方法，结束后可以随意执行该方法
    */
  def doAfter(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("aaaa")
      .set("spark.streaming.kafka.maxRatePerPartition","20")
      .set("spark.streaming.concurrentJobs","2")
    val ssc = new StreamingContext(sparkConf,Duration(500))
    val topics = Set("abner")
    val kafkaParam = Map(
      "zookeeper.connect"->"118.25.102.146:2181",
      "group.id"->"exactly-once",
      "metadata.broker.list"->"118.25.102.146:9092"
    )
    val km = new KafkaManager(kafkaParam)
    val stream  = km.createDirectStream(ssc,kafkaParam,topics)
    stream.foreachRDD((rdd,time)=>{
      if(rdd.isEmpty()){
        println("topic is empty")
      }else{
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offsets <- offsetsList) {
          //插入数据库
          KafkaOffsetDao.insetOffsetStatus(offsets.topic,offsets.partition,offsets.fromOffset,offsets.untilOffset,1)
        }
        rdd.foreachPartition( iterator =>{
          while (iterator.hasNext){
            iterator.next()
            Thread.sleep(100)
          }
          //结果一定要统一输出，要么全部成功，要么全部失败 （如果做不到，程序突然终止后重启，可能会有小部分重复）
        })
        for (offsets <- offsetsList) {
          //更新zookeeper
          km.updateZKOffsets(offsets.topic,offsets.partition,offsets.untilOffset)
          //更新结束后 删除数据
          KafkaOffsetDao.deleteOffsetStatus(offsets.topic,offsets.partition,offsets.fromOffset,offsets.untilOffset)
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 仅程序启动的时候最先执行一次
    */
  def doSomethingStart(): Unit ={
    val kafkaParam = Map(
      "zookeeper.connect"->"118.25.102.146:2181",
      "group.id"->"exactly-once",
      "metadata.broker.list"->"118.25.102.146:9092"
    )
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("aaaa")
    val sc = new SparkContext(sparkConf)
    val kafkaOffsetBeans = KafkaOffsetDao.selectOffsetStatus()
    val size = kafkaOffsetBeans.size()
    val offsetRanges = new scala.collection.mutable.ArrayBuffer[OffsetRange]()
    for(i <- 0 to size){
      val kafkaOffsetBean = kafkaOffsetBeans.get(i)
      offsetRanges.+=(OffsetRange(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getFromOffset,kafkaOffsetBean.getUntilOffset))
    }
    val rdd = KafkaUtils.createRDD[String,String,StringDecoder,StringDecoder](sc,kafkaParam,offsetRanges.toArray)
    rdd.foreachPartition( iterator =>{
      while (iterator.hasNext){
        iterator.next()
        Thread.sleep(100)
      }
    })
    //处理 结束后删除 记录
    for(i <- 0 to size){
      val kafkaOffsetBean = kafkaOffsetBeans.get(i)
      KafkaOffsetDao.deleteOffsetStatus(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getFromOffset,kafkaOffsetBean.getUntilOffset)
    }
  }
}
