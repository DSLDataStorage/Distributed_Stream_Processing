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

object Streaming{
	
	def main(args: Array[String]){

		
		val conf = new SparkConf().setMaster("mesos://192.168.0.242:5050")
		val sc = new SparkContext(conf)
		
		var printON: Boolean = false
		var printTime: Boolean = false
		var streamingIteration = 1		

		var isCogroup = true
		var enableCacheCleaningFunction = true
				
		var partition_num = args(1).toInt
		
		var cachedPRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
		var cacheTmp: org.apache.spark.rdd.RDD[(Int, String)] = null		// for cache update
				
		var DB_PRDD: org.apache.spark.rdd.RDD[(Int, String)] = null

		var LRU_RDD: org.apache.spark.rdd.RDD[(Int, Int)] = null
		var LRU_Tmp: org.apache.spark.rdd.RDD[(Int, Int)] = null			// for LRUKey update
		
		var cogroupedRDD: org.apache.spark.rdd.RDD[(Int, (Iterable[String], Iterable[String]))] = null
		var joinedPRDD_hit: org.apache.spark.rdd.RDD[(Int, (String, String))] = null
		var joinedPRDD_missed: org.apache.spark.rdd.RDD[(Int, (String, String))] = null
				
		var missedPRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
		var missedPRDD_cog_form: org.apache.spark.rdd.RDD[(Int, (Iterable[String], Iterable[String]))] = null
		var missedKeys: org.apache.spark.rdd.RDD[Int] = null
		
		var removeList: org.apache.spark.rdd.RDD[(Int, Int)] = null

		var CacheThread: Thread = null
		var RemoveListThread: Thread = null

		var missedKeysCount: Long = 0
		var pMissedKeysCount: Long = 0


		var ppMissedKeysCount: Long = 0

		var pCogTime: Long = 0
		var ppCogTime: Long = 0
		var pppCogTime: Long = 0

		var pCacheTime: Long = 0
		var ppCacheTime: Long = 0
		var pppCacheTime: Long = 0
		var ppppCacheTime: Long = 0

		var pDBTime: Long = 0
		var ppDBTime: Long = 0
		var pppDBTime: Long = 0

		var ppIterationTime:Long = 0

		var cachingWindow = 1
		var pCachingWindow = 1
		var ppCachingWindow = 1
		var pppCachingWindow = 1
		var sCachingWindow = 1
		var sCachingWindow_pre = 1
		var sCachingWindow_preTime: Long = 0
		var sCachingWindow_time: Long = 0
		var alphaValue: Long = 215
		
		
		
		var pIterationTime: Long = 0
		var pOutputCount: Long = 0

		var currCogTime: Long = 0
		var currCacheTime: Long = 0
		var currDBTime: Long = 0
		var currStreamTime: Long = 0

		var isEmpty_missedData = false

		
		var isMinus = false

		var minusValue = 1
		var plusValue = 1

		var countPlus = 0
		var countMinus = 0
		var countLB = 0
		var contPlus = false
		var contMinus = false

		var plusCase1 = 0
		var plusCase2 = 0
		var plusCase3 = 0
		var minusCase1 = 0
		var minusCase2 = 0
		var minusCase3 = 0

		var _plusCase1 = 0
		var _plusCase2 = 0
		var _plusCase3 = 0
		var _minusCase1 = 0
		var _minusCase2 = 0
		var _minusCase3 = 0

		var performedLB = false
		var globalFlag = false
		var globalCount = 0
		var isPlus = true
		var globalCacheCount: Long = 0

		var ranA = new scala.util.Random(100)

		
		var delCacheTimeList: List[Int] = null
		var isPerformed_CC_PrevIter = false

		val ssc = new StreamingContext(sc, Milliseconds(args(2).toLong))
		//ssc.checkpoint("hdfs://user-241:9000/input")
		val checkoutval = args(5).toInt

		val stream = ssc.socketTextStream("192.168.0.243", 9999)
		val stream2 = ssc.socketTextStream("192.168.0.243", 9998)
		val stream3 = ssc.socketTextStream("192.168.0.243", 9997)
		val stream4 = ssc.socketTextStream("192.168.0.243", 9996)
		//val stream5 = ssc.socketTextStream("192.168.0.243", 9995)
		//val stream6 = ssc.socketTextStream("192.168.0.243", 9994)
		/*val stream7 = ssc.socketTextStream("192.168.0.243", 9993)
		val stream8 = ssc.socketTextStream("192.168.0.243", 9992)*//*
		val stream9 = ssc.socketTextStream("192.168.0.243", 9991)
		val stream10 = ssc.socketTextStream("192.168.0.243", 9990)
		val stream11 = ssc.socketTextStream("192.168.0.243", 9989)
		val stream12 = ssc.socketTextStream("192.168.0.243", 9988)
		val stream13 = ssc.socketTextStream("192.168.0.243", 9987)
		val stream14 = ssc.socketTextStream("192.168.0.243", 9986)
		val stream15 = ssc.socketTextStream("192.168.0.243", 9985)
		val stream16 = ssc.socketTextStream("192.168.0.243", 9984)*/
		
		var streamAll = stream.union(stream2).union(stream3).union(stream4)
						//.union(stream5).union(stream6)//.union(stream7).union(stream8)
						//.union(stream9).union(stream10).union(stream11).union(stream12)
						//.union(stream13).union(stream14).union(stream15).union(stream16)



		if(args(0) == "cog"){
			isCogroup = true
		}else if(args(0) == "js"){
			isCogroup = false
		}
	
		var streaming_data_all: Int = 0
		var time_all = 0		
				
		var hashP = new HashPartitioner(partition_num)

		var mongoClient: com.mongodb.casbah.Imports.MongoClient = null
		var db: com.mongodb.casbah.MongoDB = null
		
		try{
			mongoClient = MongoClient("192.168.0.238", 27018)
			db = mongoClient("admin")
		} catch {
			case _: Throwable => {
				mongoClient = MongoClient("192.168.0.234", 27018)		
				db = mongoClient("admin")
			}
		}

		//val db = mongoClient("admin")
		/*
		var serverStatus = db.command("serverStatus").get("wiredTiger").toString
		var cacheLog = serverStatus.split('{')(5).split(",")

		if(isCogroup){
			println("cogroup, partition: " + partition_num)
		}else{
			println("join-subt, partition: " + partition_num)
		}
		println(cacheLog(38))*/

		//var hashP: org.apache.spark.HashPartitioner = null
		var preCachedFile = sc.textFile("file:///home/user/spark/cached40k.tbl", 32)
		cachedPRDD = preCachedFile.map({s=>var k = s.split('|'); var k2 = k(0).split(' '); (k2(0).toInt, k(1).toString)}).partitionBy(hashP)

		cachedPRDD.cache.count

		if(enableCacheCleaningFunction){
			LRU_RDD = cachedPRDD.map(s => (s._1, 0)).partitionBy(hashP)
			LRU_RDD.cache.count
			//println("data|LRUKey count: " + t2.cache.count + "\n")
		}		
				
		streamAll.foreachRDD({ rdd =>
			if(!rdd.isEmpty()){

				// if(streamingIteration == 12){
				// 	Thread sleep 100000000
				// }

				val tStart = System.currentTimeMillis
				var compSign = 1
				var missCount: Long = 0
				var missKeyCount: Long = 0
				isEmpty_missedData = false
				
				println()
				println("Start|Stream num: " + streamingIteration)
				//println("data|input|input count: " + rdd.count)
				
				var t0 = System.currentTimeMillis
				
				/* Discretize the input stream into RDDs */								
				var inputPRDD = rdd.map{ s=>  var tmp = s.split('|'); (tmp.lift(1).get.toInt, s) }
				//var inputPRDD = rdd.map{ s=>  var tmp = s.split('|'); (tmp.lift(0).get.toInt, s) }

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
				val missedFuture = Future{ // for missed data

					var t0 = System.currentTimeMillis
					var missedRDDThread: Thread = null
					var streamingIteration_th = streamingIteration

					if(isCogroup){
						/*
						if(streamingIteration_th < 5){
							missedPRDD_cog_form = cogroupedRDD
						}else{
							missedPRDD_cog_form = cogroupedRDD.filter{x=> x._2._2.isEmpty}
						}*/

						missedPRDD_cog_form = cogroupedRDD.filter{x=> x._2._2.isEmpty}
						
						missedPRDD_cog_form.cache

						missedRDDThread = new Thread(){
							override def run = {
								missedPRDD = missedPRDD_cog_form.flatMapValues{case(x,y)=>x}
								missedKeysCount = missedPRDD.cache.count
								println("data|mc|missedKeys count: " + missedKeysCount)
								
							}
						}						

						if(missedPRDD_cog_form.isEmpty){
							isEmpty_missedData = true
							println("data|mc|missedKeys count: 0")
						}else{
							missedRDDThread.start
						}

					}else{
						missedPRDD = inputPRDD.subtractByKey(cachedPRDD, hashP)
						missedPRDD.cache.count
						missedKeys = missedPRDD.keys.distinct	
						if(missedKeys.isEmpty){
							isEmpty_missedData = true
						}
					}

					if(!isEmpty_missedData){
/*
						var qList: List[com.mongodb.casbah.commons.Imports.DBObject] = null
						var mongoClient = MongoClient("192.168.0.9", 27020)
						//var mongoClient = MongoClient("192.168.0.243", 27017)
						//var dbData = Array("a")
						val db = mongoClient("n4s10000h")
						var coll = db("part")

						var count = 0
						missedPRDD_cog_form.collect.foreach{ s => var tmp = MongoDBObject("partkey" -> s); 
							if(count == 0){ qList = List(tmp) }
							else{ qList = qList :+ tmp }; 
						count = count + 1 }

						var q = MongoDBObject("$or" -> qList)

						var documents = coll.find(q)

						var tmp = Array("a")

						documents.foreach(s => tmp = tmp:+ s.toString)

						var tmpAll = tmp.drop(1)

						var newRDD = sc.parallelize(tmpAll)
						//var newPRDD = newRDD.map{ s=> {var tmpAll=s.split(" "); (tmpAll.lift(10).get.toInt, s) } }.partitionBy(new HashPartitioner(16))
						DB_PRDD = newRDD.map{ s=> {var tmpAll=s.split(" "); (tmpAll.lift(10).get.toInt, s) } }.partitionBy(new HashPartitioner(16))
*/

						
						DB_PRDD = missedPRDD_cog_form.mapPartitions({ iter =>						
							var count = 0
							var qList: List[com.mongodb.casbah.commons.Imports.DBObject] = null
							var mongoClient = MongoClient("192.168.0.9", 27020)
							//var mongoClient = MongoClient("192.168.0.243", 27017)
							var dbData = Array("a")

							val db = mongoClient(args(3))

							//val db = mongoClient("n4s100h")
							//val db = mongoClient("n4s10000h")
							//val db = mongoClient("s10000")
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
						
						
						var DB_count = DB_PRDD.cache.count
						DB_count										

						t1 = System.currentTimeMillis
						println("time|4|create query + get data + create new RDD: " + (t1 - t0) + " ms")
						currDBTime = t1 - t0

						//CacheThread.start
						RemoveListThread.start
											
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
					else{						
						println("time|4|create query + get data + create new RDD: 0 ms")
						currDBTime = 0
						RemoveListThread.start
						println("data|jm|joined_miss count: 0")
						println("time|5|join - miss data: 0 ms")
					}
				} // Future

// cache management thread - update LRU keys
				var LRUKeyThread = new Thread(){
					override def run = {
						var t0 = System.currentTimeMillis

						var streamingIteration_th = streamingIteration
						var cachingWindow_th = cachingWindow
						var threshold_prev = streamingIteration_th - cachingWindow_th - 1

						var inputKeysRDD = cogroupedRDD.mapPartitions({ iter =>
							var newPartition = iter.map(s => (s._1, streamingIteration_th))
							newPartition
						}, preservesPartitioning = true)


						if(isPerformed_CC_PrevIter){
							LRU_Tmp = LRU_RDD
											.filter(s => s._2 >= threshold_prev)
											.subtractByKey(inputKeysRDD, hashP)
											.union(inputKeysRDD)

							isPerformed_CC_PrevIter = false

						}else{
							LRU_Tmp = LRU_RDD
											.subtractByKey(inputKeysRDD, hashP)
											.union(inputKeysRDD)
						}

						if(streamingIteration_th % checkoutval == 0){
							LRU_Tmp.localCheckpoint
						}

						LRU_Tmp.cache.count
						LRU_RDD.unpersist()
						LRU_RDD = LRU_Tmp

						var t1 = System.currentTimeMillis
						println("time|9|LRU keys update time: " + (t1 - t0) + " ms")					
					}
				}

				RemoveListThread = new Thread(){
					override def run = {
						t0 = System.currentTimeMillis

						var delCacheTimeList_th = delCacheTimeList 
						var enableCacheCleaningFunction_th = enableCacheCleaningFunction
						var streamingIteration_th = streamingIteration						

						var cachingWindow_th = cachingWindow
						var currCogTime_th = currCogTime
						var currDBTime_th = currDBTime

						var pCacheRelatedOpTimeDiff = currCogTime_th - pCogTime + pCacheTime - ppCacheTime
						var pDBTimeDiff = currDBTime_th - pDBTime

						println("data|cwb|caching window size: " + cachingWindow_th)
						
						var pAll = currCogTime_th + currDBTime_th + pCacheTime
						var ppAll = pCogTime + pDBTime + ppCacheTime
						var pppAll = ppCogTime + ppDBTime + pppCacheTime
						var isEmpty_missedData_th = isEmpty_missedData


						
						if(isEmpty_missedData_th){
							cachingWindow_th += 1
							//cachingWindow_th = streamingIteration_th
							sCachingWindow = cachingWindow_th
						}
						else if(streamingIteration_th > args(4).toInt){


							if(pAll > ppAll){
								
										cachingWindow_th = sCachingWindow
		
							}else if(pAll < ppAll){

								sCachingWindow = cachingWindow_th
								
								if(currDBTime > currCogTime + pCacheTime){

									cachingWindow_th += 1
															
								}else if(currDBTime < currCogTime + pCacheTime && cachingWindow_th > 1){

									cachingWindow_th -= 1
											
								}							
							}
						}else{
							cachingWindow_th += 1
							sCachingWindow = cachingWindow_th
						}

						if(cachingWindow_th > 140){
						 	cachingWindow_th = 130
						 	sCachingWindow = 129
						}



						println("data|cwa|caching window size: " + cachingWindow_th)

						var threshold_curr = streamingIteration_th - cachingWindow_th

						LRUKeyThread.join()

						//if(streamingIteration_th > 419){
							removeList = LRU_RDD.filter({ s => s._2 < threshold_curr })
						//}					

						this.synchronized{
							cachingWindow = cachingWindow_th
						}

						CacheThread.start
					}
				}

// cache management thread - update cachedRDD
				CacheThread = new Thread(){
					override def run = {						
						var enableCacheCleaningFunction_th = enableCacheCleaningFunction
						var streamingIteration_th = streamingIteration
						var isEmpty_missedData_th = isEmpty_missedData

						var t0: Long = System.currentTimeMillis

						//var flag = false

						if(enableCacheCleaningFunction_th == false){ // disable cache cleaning
							cacheTmp = cachedPRDD.union(DB_PRDD).partitionBy(hashP)
						}else{
							
							//RemoveListThread.join()
							if(isEmpty_missedData_th){
								cacheTmp = cachedPRDD
							}
							else if(!removeList.isEmpty){
								cacheTmp = cachedPRDD.subtractByKey(removeList, hashP).union(DB_PRDD)
								//flag = true
								isPerformed_CC_PrevIter = true
							}else{
								cacheTmp = cachedPRDD.union(DB_PRDD)
							}
						}

						if(streamingIteration % checkoutval == 0){
							println("~~~~~~~~  localCheckpoint ~~~~~~~~")
							cacheTmp.localCheckpoint
						}

						var cachedDataCount = cacheTmp.cache.count
						globalCacheCount = cachedDataCount
						println("data|c|cachedData: " + cachedDataCount)

						cachedPRDD.unpersist()
						cachedPRDD = cacheTmp

						var t1 = System.currentTimeMillis
						println("time|6|create cachedPRDD: " + (t1 - t0) + " ms")
						
						currCacheTime = t1 - t0
					}
				}

// ------------------------------------------------ main thread ------------------------------------------------ 
				if(enableCacheCleaningFunction){ 
					LRUKeyThread.start
					//RemoveListThread.start
				}
				hitThread.start

				// start Future missedFuture (missed data join)
				t0 = System.currentTimeMillis
				val n =Await.result(missedFuture, scala.concurrent.duration.Duration.Inf)
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
				
				CacheThread.join()

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

				pppCogTime = ppCogTime
				ppCogTime = pCogTime
				pCogTime = currCogTime

				pppDBTime = ppDBTime
				ppDBTime = pDBTime
				pDBTime = currDBTime

				ppppCacheTime = pppCacheTime				
				pppCacheTime = ppCacheTime
				ppCacheTime = pCacheTime
				
				ppIterationTime = pIterationTime
				
				ppMissedKeysCount = pMissedKeysCount

				ppCachingWindow = pCachingWindow
				pCachingWindow = cachingWindow
				
				pCacheTime = currCacheTime
				
				pOutputCount = outputCount
				pIterationTime = currStreamTime
				pMissedKeysCount = missedKeysCount
				
				streaming_data_all = streaming_data_all + outputCount.toInt
				println("data|all|streaming data all: " + streaming_data_all)
				
				//var timetmp: Int = tEnd.toInt - tStart.toInt
				//time_all = time_all + timetmp				
				//println("time|0|total: " + time_all + " ms")
				/*
				var serverStatus = db.command("serverStatus").get("wiredTiger").toString
				var cacheLog = serverStatus.split('{')(5).split(",")
				println(cacheLog(5))
				println(cacheLog(10))*/
								
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