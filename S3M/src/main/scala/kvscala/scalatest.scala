package kvscala

import kvmatch.test
import kvmatch.common.Pair
import kvmatch.Dataread

import scala.collection.JavaConversions._
import collection.JavaConverters._
import java.util.List;
import org.apache.spark.sql.{Encoder, Encoders}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD


object scalatest{ 

  def main(args: Array[String]):Unit = {
    val sel = args(0).toInt
    val n = args(1).toInt
    val epsilon = args(2).toDouble
    val rho = args(3).toInt
    val alpha = args(4).toDouble
    val beta = args(5).toDouble

    val pred:Int = args(6).toInt
    val pred_idx:Int = args(7).toInt
    val window_size: Int = args(8).toInt
    val window_size_t: Int = args(9).toInt
    val window_interval: Int = args(10).toInt
    val warm: Int = args(11).toInt
    val run: Int = args(12).toInt

    val LR: Double = args(13).toDouble
    val NumIter: Int = args(14).toInt
    val RegParam: Double = args(15).toDouble
    val BatchFr: Double = args(16).toDouble

    val IP: String = args(17).toString
    val file_name = args(18).toString

    val jtest = new test(n, file_name)
    val dread = new Dataread()

    println("create model")
    var model = new StreamingLinearRegressionWithSGD_dsl()
    .setStepSize(LR)
    .setNumIterations(NumIter)
    .setRegParam(RegParam)
    .setMiniBatchFraction(BatchFr)
    .setInitialWeights(Vectors.zeros(window_size-1+pred))
    println("created model")

    val conf = new SparkConf().setAppName("kvmatch_test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Milliseconds(window_interval))

    var streamingIteration = 1
    var cnt:Long = 0
    var miss = 0
    var sum_MSE:Double = 0

    var train_rows = sc.parallelize(Seq(LabeledPoint(0.toDouble, Vectors.zeros(window_size-1+pred)))).cache()

    val stream = ssc.socketTextStream(IP, 9999)
    val start_total = System.currentTimeMillis

    stream.window(Milliseconds(window_size_t)).foreachRDD({ rdd =>
      cnt = rdd.count()
      if(!rdd.isEmpty() && cnt>window_size){
        println("\n\nStart|Stream num: " + streamingIteration)

        var Start = System.currentTimeMillis

        val price = rdd.map(line => line.split(",")(0).toDouble)
        val future_price = rdd.map(line => line.split(",")(pred_idx).toDouble)
        var sequence = price.take(window_size) ++ future_price.take(window_size)

        var roc:java.util.List[java.lang.Double] = new java.util.ArrayList[java.lang.Double]()
        var future_roc :java.util.List[java.lang.Double] = new java.util.ArrayList[java.lang.Double]()

        for(i<-0 to window_size-2)
        {
          roc.add(sequence(i+1)-sequence(i))
        }
        for(i<-2*window_size-pred to 2*window_size-1)
        {
          future_roc.add(sequence(i)-sequence(i-1))
        }
        
        var pred_y:Double = sequence(2*window_size-1)-sequence(window_size-1)

        var rows = sc.parallelize(roc).map(x=>x.toDouble)

        val answer = jtest.execute(sel, n, epsilon, rho, alpha, beta, roc)  //query engine, answer = list<pair<int, double>> offset, distance
        var sim_rows:org.apache.spark.rdd.RDD[Double] = null
        if(answer.isEmpty())
        {
          sim_rows = sc.parallelize(future_roc).map(x=>x.toDouble)
          miss = miss+1
        }
        else
        {
          if(answer.get(0).getFirst() + window_size + pred >= n)
          {
            miss = miss+1
            println("EOF")
            sim_rows = sc.parallelize(future_roc).map(x=>x.toDouble)
          }
          else
          {
            sim_rows = sc.parallelize(dread.load(answer.get(0).getFirst() + window_size, pred, file_name)).map(x=>x.toDouble)  //load from file
          }
        }

        var test_int_rows: org.apache.spark.rdd.RDD[(Double, Vector)] = sc.parallelize(Seq((pred_y, Vectors.dense(rows.collect() ++ sim_rows.collect())))).cache()
                
        println("data|qc|query_count : " + cnt)

          /* model train */
        var tStart = System.currentTimeMillis
        if(streamingIteration>1)
        {
          model.trainOn_dsl(train_rows)
        }
        var tEnd = System.currentTimeMillis
        println("time|train: "+(tEnd - tStart)+" ms")
        
        train_rows.unpersist()
        train_rows = sc.parallelize(Seq(LabeledPoint(pred_y, Vectors.dense(rows.collect() ++ sim_rows.collect())))).cache()

        /* model test */
        tStart = System.currentTimeMillis
        val MSE = model.predictOnValues_dsl(test_int_rows).map{case(x,y) => math.pow((x - y),2)}.mean()
        

        println("traing Mean Squared Error "+MSE)
        tEnd = System.currentTimeMillis

        println("time|prediction: "+(tEnd - tStart)+" ms")
        test_int_rows.unpersist()

        rdd.unpersist()

        var End = System.currentTimeMillis

        println("time|latency: "+(End - Start)+" ms")
        streamingIteration = streamingIteration + 1

        if(( tEnd - start_total) > warm ){
            sum_MSE = sum_MSE + MSE
        }

        if(( tEnd - start_total) > run ){
            ssc.stop()
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
    var end_total = System.currentTimeMillis
    var total_time = end_total - start_total
    println("\n\n=============Result================")
    println("time|total_time: "+(total_time)+" ms")
    println("miss|miss_match: "+(miss))
    println("MSE|total_mse: "+(sum_MSE/(streamingIteration-warm/window_interval)))


  }
}
