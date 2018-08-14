# SparkKafkaExectlyOnce
use other db storage offset
use createrdd api deal the failed message
val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      // get this info from db
      OffsetRange("testCluster", 0, 0, 10),
      OffsetRange("testCluster", 1, 0, 1)
    )
    val map = new java.util.HashMap[String,Object]()
    map.put("bootstrap.servers","host:9092")
    map.put("key.deserializer",classOf[StringDeserializer])
    map.put("value.deserializer",classOf[StringDeserializer])
    val rdd = KafkaUtils.createRDD[String, String](sc, map, offsetRanges, LocationStrategies.PreferConsistent)
    rdd.foreach(x=>{
      println("mmmmmm : "+x.key())
      println("mmmmmmm : "+x.value())
    })
  }
use sparkstreaming api deal message
val scc = new StreamingContext(sparkConf,Duration(500))
    val topicAndPartitions = Set(new TopicPartition("testCluster",0),new TopicPartition("testCluster",1),new TopicPartition("testCluster",2))
    val fromOffsets = Map(new TopicPartition("testCluster",0)->0L, new TopicPartition("testCluster",1)->0L, new TopicPartition("testCluster",2)->0L)
    val stream =  KafkaUtils.createDirectStream[String, String](scc,
      LocationStrategies.PreferConsistent,ConsumerStrategies.Assign[String, String](topicAndPartitions, kafkaParam2, fromOffsets))
    println("==================="+stream.slideDuration)
    stream.foreachRDD(rdd =>{
      rdd.foreach(record =>{
        println(record.key())
        println(record.value())
        Thread.sleep(200)
        println("!!!!!!!!!!!!!!!!!!!!!!1"+record)
      })
      // storage the offset in db
    })
    scc.start()
    scc.awaitTermination()
