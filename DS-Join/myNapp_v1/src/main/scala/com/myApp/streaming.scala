import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import com.mongodb.casbah.Imports.MongoDBObject
import com.mongodb.casbah.Imports.MongoClient
import com.mongodb.casbah.Imports.DBObject

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.util.control.Breaks._

import akka.actor._

object Streaming{
	

	def main(args: Array[String]){
		
		//conf.set("spark.locality.wait", " 10ms")
		var hashP: org.apache.spark.HashPartitioner = null
		val conf = new SparkConf().setMaster("mesos://192.168.0.242:5050")
		val sc = new SparkContext(conf)
		
		var printON: Boolean = false
		var printTime: Boolean = false
		var streamingIteration = 1		

		var isCogroup = true
		var enableCacheCleaningFunction = true
		
		if(args(0) == "cog"){
			isCogroup = true
		}else if(args(0) == "js"){
			isCogroup = false
		}

		var partition_num = args(1).toInt
		
		var cachedPRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
		var cacheTmp: org.apache.spark.rdd.RDD[(Int, String)] = null
		var LRUKeyTmp: org.apache.spark.rdd.RDD[(Int, Int)] = null
		var missedPRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
		var DB_PRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
		var LRUKeysRDD: org.apache.spark.rdd.RDD[(Int, Int)] = null
		var LRUKey2: org.apache.spark.rdd.RDD[(Int, Int)] = null
		
		var joinedPRDD_hit: org.apache.spark.rdd.RDD[(Int, (String, String))] = null
		var joinedPRDD_missed: org.apache.spark.rdd.RDD[(Int, (String, String))] = null
		var cogroupedRDD: org.apache.spark.rdd.RDD[(Int, (Iterable[String], Iterable[String]))] = null
		var loj: org.apache.spark.rdd.RDD[(Int, (String, Option[String]))] = null
		var missedPRDD_cog_form: org.apache.spark.rdd.RDD[(Int, (Iterable[String], Iterable[String]))] = null
		var missedKeys: org.apache.spark.rdd.RDD[Int] = null
		var delLRUKeys: org.apache.spark.rdd.RDD[(Int, Int)] = null

		var cacheThread: Thread = null

		var cachedDataCount_new: Long = 0
		var cachedDataCount_old: Long = 0

		var preCogTime: Long = 0
		var preCacheTime: Long = 0
		var preDBTime: Long = 0
		var pre2DBTime: Long = 0
		var preStreamTime: Long = 0
		var pre2StreamTime: Long = 0
		var preCacheRelatedOpTimeDiff: Long = 0

		var currCogTime: Long = 0
		var currCacheTime: Long = 0
		var currDBTime: Long = 0
		var currStreamTime: Long = 0
		var properCachedDataCount: Long = 40000
		var properCachedDataSet: Long = 10

		var isEmpty_missedData = false
		var numDelCacheCount = 1

		
		var delCacheNum = 0
		
		var delCacheTimeList: List[Int] = null
		var isPerformed_CC_PrevIter = false

		val ssc = new StreamingContext(sc, Seconds(1))
		//ssc.checkpoint("hdfs://user-241:9000/input")

		val stream = ssc.socketTextStream("192.168.0.243", 9999)
		val stream2 = ssc.socketTextStream("192.168.0.243", 9998)
		val stream3 = ssc.socketTextStream("192.168.0.243", 9997)
		val stream4 = ssc.socketTextStream("192.168.0.243", 9996)
		val stream5 = ssc.socketTextStream("192.168.0.243", 9995)
		val stream6 = ssc.socketTextStream("192.168.0.243", 9994)
		val stream7 = ssc.socketTextStream("192.168.0.243", 9993)
		val stream8 = ssc.socketTextStream("192.168.0.243", 9992)/*
		val stream9 = ssc.socketTextStream("192.168.0.243", 9991)
		val stream10 = ssc.socketTextStream("192.168.0.243", 9990)
		val stream11 = ssc.socketTextStream("192.168.0.243", 9989)
		val stream12 = ssc.socketTextStream("192.168.0.243", 9988)
		val stream13 = ssc.socketTextStream("192.168.0.243", 9987)
		val stream14 = ssc.socketTextStream("192.168.0.243", 9986)
		val stream15 = ssc.socketTextStream("192.168.0.243", 9985)
		val stream16 = ssc.socketTextStream("192.168.0.243", 9984)*/
		
		//var streamAll = sc.union(stream, stream2, stream3, stream4, stream5, stream6, stream7, stream8)
		
		var streamAll = stream.union(stream2).union(stream3).union(stream4)
						.union(stream5).union(stream6).union(stream7).union(stream8)
						//.union(stream9).union(stream10).union(stream11).union(stream12)
						//.union(stream13).union(stream14).union(stream15).union(stream16)

	
		var streaming_data_all: Int = 0
		var time_all = 0		
				
		hashP = new HashPartitioner(partition_num)

		var mongoClient = MongoClient("192.168.0.238", 27018)		
		val db = mongoClient("admin")
		var serverStatus = db.command("serverStatus").get("wiredTiger").toString
		var cacheLog = serverStatus.split('{')(5).split(",")

		
		if(isCogroup){
			println("cogroup, partition: " + partition_num)
		}else{
			println("join-subt, partition: " + partition_num)
		}
		println(cacheLog(38))

		var preCachedFile = sc.textFile("file:///home/user/spark/cached40k.tbl", 32)
		cachedPRDD = preCachedFile.map({s=>var k = s.split('|'); var k2 = k(0).split(' '); (k2(0).toInt, k(1).toString)}).partitionBy(hashP)
		cachedPRDD.cache
		cachedDataCount_old = cachedPRDD.count
		cachedDataCount_old

		if(enableCacheCleaningFunction){
			LRUKeysRDD = cachedPRDD.map(s => (s._1, 0)).partitionBy(hashP)
			LRUKeysRDD.cache.count
			//println("data|LRUKey count: " + t2.cache.count + "\n")
		}		
				
		streamAll.foreachRDD({ rdd =>
			if(!rdd.isEmpty()){
/*
				if(streamingIteration == 10){
					partition_num = 8
					hashP = new HashPartitioner(partition_num)
				}
*/
/*
				if(streamingIteration == 12){
					Thread sleep 100000000
				}
*/
				val tStart = System.currentTimeMillis
				var compSign = 1
				var missCount: Long = 0
				var missKeyCount: Long = 0
				
				println()
				println("Start|Stream num: " + streamingIteration)
				
				var t0 = System.currentTimeMillis
				
				/* Discretize the input stream into RDDs */								
				var inputPRDD = rdd.map{ s=>  var tmp = s.split('|'); (tmp.lift(1).get.toInt, s) }

				if(isCogroup){
					cogroupedRDD = inputPRDD.cogroup(cachedPRDD).filter{s=>(!s._2._1.isEmpty)}
					cogroupedRDD.cache
					cogroupedRDD.count
				}else{
					inputPRDD.cache
					inputPRDD.count
				}

				var t1 = System.currentTimeMillis

				println("time|1|cogroup (input-cached): " + (t1 - t0) + " ms")
				currCogTime = t1 - t0


/* cache management thread - update LRU keys */
				var LRUKeyThread = new Thread(){
					override def run = {
						var t0 = System.currentTimeMillis

						var streamingIteration_th = streamingIteration
						var delCacheTimeList_th = delCacheTimeList 

						var inputKeysRDD = cogroupedRDD.mapPartitions({ iter =>
							var newPartition = iter.map(s => (s._1, streamingIteration_th))
							newPartition
						}, preservesPartitioning = true)

						if(isPerformed_CC_PrevIter){
							LRUKeyTmp = LRUKeysRDD
											.filter(s => !delCacheTimeList_th.contains(s._2))
											.subtractByKey(inputKeysRDD, hashP)
											.union(inputKeysRDD)

							isPerformed_CC_PrevIter = false							
						}else{
							LRUKeyTmp = LRUKeysRDD
											.subtractByKey(inputKeysRDD, hashP)
											.union(inputKeysRDD)
						}

						if(streamingIteration_th % 30 == 0){
							LRUKeyTmp.localCheckpoint
						}

						LRUKeyTmp.cache.count
						LRUKeysRDD.unpersist()
						LRUKeysRDD = LRUKeyTmp

						var t1 = System.currentTimeMillis
						println("time|9|LRU keys update time: " + (t1 - t0) + " ms")
					}
				}				


/* hit data join thread */
				val hitThread = new Thread(){
					override def run = {
						var t0 = System.currentTimeMillis
						
						if(isCogroup){
							joinedPRDD_hit = cogroupedRDD.filter{s=> (!s._2._2.isEmpty)}
												.flatMapValues(pair => for(v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w))
						}else{
							joinedPRDD_hit = inputPRDD.join(cachedPRDD)
						}
												
						joinedPRDD_hit.cache
						println("data|jh|joined Hit count: " + joinedPRDD_hit.count)				
						
						var t1 = System.currentTimeMillis
						println("time|3|join-hit data: " + (t1 - t0) + " ms")
					}
				}
				
	
/* miss data join thread */
				val f = Future{ // for missed data

					var t0 = System.currentTimeMillis
					var missedRDDThread: Thread = null

					if(isCogroup){
						missedPRDD_cog_form = cogroupedRDD.filter{x=> x._2._2.isEmpty}
						missedPRDD_cog_form.cache

						missedRDDThread = new Thread(){
							override def run = {
								missedPRDD = missedPRDD_cog_form.flatMapValues{case(x,y)=>x}
								missedPRDD.cache.count
							}
						}
						missedRDDThread.start

						if(missedPRDD_cog_form.isEmpty){
							isEmpty_missedData = true
						}

					}else{
						missedPRDD = inputPRDD.subtractByKey(cachedPRDD, hashP)
						missedPRDD.cache.count
						missedKeys = missedPRDD.keys.distinct	
						if(missedKeys.isEmpty){
							isEmpty_missedData = true
						}
					}
/*
					var missedKeyCount = missedKeys.count
					println("data|mk|missed keys count: " + missedKeyCount)
*/					
					if(!isEmpty_missedData){
						DB_PRDD = missedPRDD_cog_form.mapPartitions({ iter =>						
							var count = 0
							var qList: List[com.mongodb.casbah.commons.Imports.DBObject] = null
							var mongoClient = MongoClient("192.168.0.9", 27020)
							var dbData = Array("a")

							val db = mongoClient("n8s10000h")
							var coll = db("part")

							if(!iter.isEmpty){
								iter.foreach{ case(k, (v1, v2)) =>
									var tmp = MongoDBObject("partkey" -> k);
									if(count == 0){
										qList = List(tmp)
										count = 1
									}else{
										qList = qList:+tmp
									}
								}
								var q = MongoDBObject("$or" -> qList)
								coll.find(q).foreach(s=> dbData = dbData :+ s.toString)
							}

							mongoClient.close
		
							dbData = dbData.drop(1)
							var db_arr = dbData.map{ s=> {var tmpAll=s.split(" "); (tmpAll.lift(10).get.toInt, s)} }
							db_arr.iterator
						}, preservesPartitioning = true)
						
						var DB_count = DB_PRDD.persist.count
						DB_count

						t1 = System.currentTimeMillis
						println("time|4|create query + get data + create new RDD: " + (t1 - t0) + " ms")
						currDBTime = t1 - t0


						cacheThread.start
											
						/* join missed data */
						t0 = System.currentTimeMillis
						
						if(isCogroup){
							missedRDDThread.join()
						}

						joinedPRDD_missed = missedPRDD.join(DB_PRDD, hashP)
						
						joinedPRDD_missed.cache
						var joinedPRDD_missed_count = joinedPRDD_missed.count
						println("data|jm|joined_miss count: " + joinedPRDD_missed_count)
											
						missedPRDD.unpersist()

						t1 = System.currentTimeMillis
						println("time|5|join - miss data: " + (t1 - t0) + " ms")

						//cacheThread.join()					

					}  	// missed 0 exception
				} // Future

/* cache management thread - update cachedRDD */
				cacheThread = new Thread(){
					override def run = {						
						var enableCacheCleaningFunction_th = enableCacheCleaningFunction
						var streamingIteration_th = streamingIteration

						var t0: Long = System.currentTimeMillis

						if(enableCacheCleaningFunction_th == false){ // disable cache cleaning
							cacheTmp = cachedPRDD.union(DB_PRDD).partitionBy(hashP)
						}else{
							var isContinouousDelCache = false
							var performCacheCleaning = false
							var delCacheTimeList_th = delCacheTimeList
							var remainNumCachedCount = streamingIteration - delCacheNum
							var cacheRelatedOpTimeDiff = currCogTime - preCogTime + currCacheTime - preCacheTime// cache update
							var DBOpTimeDiff = currDBTime - preDBTime
							var preNewCachedDataCount = cachedDataCount_new  // curr cachedDataCount
							var preOldCachedDataCount = cachedDataCount_old

							if(cacheRelatedOpTimeDiff > 0){
								if(DBOpTimeDiff < 0){ 
									DBOpTimeDiff = DBOpTimeDiff * (-1) 
								}
								if(cacheRelatedOpTimeDiff > DBOpTimeDiff){
									performCacheCleaning = true
								}
							}else{
								if(preCacheRelatedOpTimeDiff > cacheRelatedOpTimeDiff*(-1)){
									performCacheCleaning = true
								}
							}

							preCacheRelatedOpTimeDiff = cacheRelatedOpTimeDiff

							if(streamingIteration_th < 10){
								performCacheCleaning = false
							}

							
						
							if(performCacheCleaning){

								if(preNewCachedDataCount > properCachedDataCount){
									numDelCacheCount += 1
								}else{
									if(numDelCacheCount != 1){
										numDelCacheCount -= 1
									}									
								}

								if(!isContinouousDelCache){
									properCachedDataCount = (properCachedDataCount + preOldCachedDataCount) / 2
								}

								if(remainNumCachedCount <= numDelCacheCount){
									numDelCacheCount = 1
								}

								println("data|dc|num delete cache count: " + numDelCacheCount)

								delCacheTimeList_th = List.range(delCacheNum, delCacheNum + numDelCacheCount)

								delCacheNum += numDelCacheCount // start time to remove from cachedRDD

								this.synchronized{
									delCacheTimeList = delCacheTimeList_th
								}

								LRUKeyThread.join()

								t0 = System.currentTimeMillis
								delLRUKeys = LRUKeysRDD.filter({ s => delCacheTimeList_th.contains(s._2);})
														
								cacheTmp = cachedPRDD.subtractByKey(delLRUKeys, hashP).union(DB_PRDD)//.partitionBy(hashP)
		
								isPerformed_CC_PrevIter = true
								
								isContinouousDelCache = true
							}else{
								t0 = System.currentTimeMillis
								cacheTmp = cachedPRDD.union(DB_PRDD)

								isContinouousDelCache = false
							}
						}

						if(streamingIteration % 30 == 0){
							cacheTmp.localCheckpoint
						}
						
						cachedDataCount_old = cachedDataCount_new

						cachedDataCount_new = cacheTmp.cache.count
						println("data|c|cachedData: " + cachedDataCount_new)

						cachedPRDD.unpersist()
						cachedPRDD = cacheTmp

						var t1 = System.currentTimeMillis
						println("time|6|create cachedPRDD: " + (t1 - t0) + " ms")
						currCacheTime = t1 - t0
					}
				}


/* ------------------------------------------------ main thread ------------------------------------------------ */
				if(enableCacheCleaningFunction){ 
					LRUKeyThread.start 
				}
				hitThread.start



				// start Future f (missed data join)
				t0 = System.currentTimeMillis
				val n =Await.result(f, scala.concurrent.duration.Duration.Inf)
				t1 = System.currentTimeMillis
				//println("time|9|missed Thread: " + (t1 - t0) + " ms")	
				
				hitThread.join() // hit thread
								
				var outputCount: Long = 0
										
				/* union (joinedRDD_hit, joinedRDD_missed) */
				t0 = System.currentTimeMillis
				if(isEmpty_missedData){
					outputCount = joinedPRDD_hit.count
				}else{
					outputCount = joinedPRDD_hit.union(joinedPRDD_missed).count								
				}				
				println("data|out|output data: " + outputCount)				
				t1 = System.currentTimeMillis
				println("time|7|union output data: " + (t1 - t0) + " ms")			
				

				cacheThread.join()


				streamingIteration = streamingIteration + 1
				//t0 = System.currentTimeMillis
				
				/* unpersist */
				if(isCogroup){
					cogroupedRDD.unpersist()
					missedPRDD_cog_form.unpersist()
				}else{
					inputPRDD.unpersist()
				}
				rdd.unpersist()
				DB_PRDD.unpersist()

				val tEnd = System.currentTimeMillis
				currStreamTime = tEnd - tStart
				println("time|8|stream: " + currStreamTime + " ms")

				preCogTime = currCogTime
				preCacheTime = currCacheTime
				pre2DBTime = preDBTime
				preDBTime = currDBTime
				pre2StreamTime = preStreamTime
				preStreamTime = currStreamTime

				
				streaming_data_all = streaming_data_all + outputCount.toInt
				println("data|all|streaming data all: " + streaming_data_all)
				
				var timetmp: Int = tEnd.toInt - tStart.toInt
				time_all = time_all + timetmp
				
				println("time|0|total: " + time_all + " ms")

				var serverStatus = db.command("serverStatus").get("wiredTiger").toString
				var cacheLog = serverStatus.split('{')(5).split(",")
				println(cacheLog(5))
				println(cacheLog(10))
								
				//var throughput = streaming_data_all * 1000 / time_all
				//println("END|throughput: " + throughput)

				joinedPRDD_hit.unpersist()
				if(!isEmpty_missedData){
					joinedPRDD_missed.unpersist()
				}

			}
		})
		
		ssc.start()
		ssc.awaitTermination()
	}
}