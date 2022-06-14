package kvscala

import kvmatch.test
import kvmatch.common.Pair
import kvmatch.Dataread

import scala.collection.mutable._
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


object s3m{ 

    def main(args: Array[String]):Unit = {
        val sel = args(0).toInt
        val n = args(1).toInt
        val epsilon = args(2).toDouble
        val rho = args(3).toInt
        val alpha = args(4).toDouble
        val beta = args(5).toDouble

        val pred:Int = args(6).toInt
        val window_size: Int = args(7).toInt
        val window_size_t: Int = args(8).toInt
        val window_interval: Int = args(9).toInt
        val warm: Int = args(10).toInt
        val run: Int = args(11).toInt

        val LR: Double = args(12).toDouble
        val NumIter: Int = args(13).toInt
        val RegParam: Double = args(14).toDouble
        val BatchFr: Double = args(15).toDouble

        val IP: String = args(16).toString
        val file_name = args(17).toString

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

        val conf = new SparkConf().setAppName("s3m")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        val ssc = new StreamingContext(sc, Milliseconds(window_interval))

        var streamingIteration = 1
        var num_train = 0
        var cnt:Long = 0
        var miss = 0
        var miss_end = 0
        var sum_MSE:Double = 0
        val WI:Int = 120/(window_size_t/1000)
        val pred_idx:Int=(pred-1)%window_size
        val q_size:Int=(pred/WI).ceil.toInt

        var tr_queue = new Queue[Vector]

        val stream = ssc.socketTextStream(IP, 9999)
        val start_total = System.currentTimeMillis

        stream.window(Milliseconds(window_size_t)).foreachRDD({ rdd =>
            cnt = rdd.count()
            if(!rdd.isEmpty() && cnt>window_size){
                println("\n\nStart|Stream num: " + streamingIteration)

                var Start = System.currentTimeMillis

                val sequence = rdd.map(line => line.toDouble).take(window_size)

                var Qs:java.util.List[java.lang.Double] = new java.util.ArrayList[java.lang.Double]()

                for(i<-0 to window_size-2)
                {
                    Qs.add(sequence(i+1)-sequence(i))
                }
                
                var Qs_rdd = sc.parallelize(Qs).map(x=>x.toDouble)

                val answer = jtest.execute(sel, n, epsilon, rho, alpha, beta, Qs)  //query engine, answer = list<pair<int, double>> offset, distance
                if(answer.isEmpty())
                {
                    miss = miss+1
                    print("miss!")
                    tr_queue.enqueue(null)
                }
                else
                {
                    if(answer.get(0).getFirst() + window_size + pred >= n)
                    {
                        miss_end = miss_end+1
                        println("EOF")
                        tr_queue.enqueue(null)
                    }
                    else
                    {
                        var Ds = sc.parallelize(dread.load(answer.get(0).getFirst() + window_size, pred, file_name)).map(x=>x.toDouble)  //load from file
                        tr_queue += Vectors.dense(Qs_rdd.collect() ++ Ds.collect())
                    }
                }

                if(tr_queue.size > q_size)
                {
                    var queue_row = tr_queue.dequeue

                    if(queue_row != null)
                    {
                        var pred_y:Double = sequence(pred_idx)
                        var train_row = sc.parallelize(Seq(LabeledPoint(pred_y, queue_row))).cache()
                        var test_row = sc.parallelize(Seq((pred_y, queue_row)))

                        println("data|qc|query_count : " + cnt)

                        /* model test */
                        var tStart = System.currentTimeMillis
                        val MSE = model.predictOnValues_dsl(test_row).map{case(x,y) => math.pow((x - y),2)}.mean()
                        var tEnd = System.currentTimeMillis
                        println("traing Mean Squared Error "+MSE)
                        println("time|prediction: "+(tEnd - tStart)+" ms")

                        /* model train */
                        tStart = System.currentTimeMillis
                        model.trainOn_dsl(train_row)
                        tEnd = System.currentTimeMillis
                        println("time|train: "+(tEnd - tStart)+" ms")
                        
                        train_row.unpersist()
                        rdd.unpersist()

                        var End = System.currentTimeMillis

                        println("time|latency: "+(End - Start)+" ms")

                        if(( tEnd - start_total) > warm ){
                            sum_MSE = sum_MSE + MSE
                            num_train = num_train+1
                        }
                        streamingIteration = streamingIteration + 1

                        if(( tEnd - start_total) > run ){
                            ssc.stop()
                        }
                    }
                }
            }
        })

        ssc.start()
        ssc.awaitTermination()
        var end_total = System.currentTimeMillis
        var total_time = end_total - start_total
        println("\n\n=============Result================")
        println("time|total_time: "+(total_time)+" ms")
        println("miss|miss_match: "+(miss)+","+(miss_end))
        println("train|num_train: "+(num_train))
        println("MSE|total_mse: "+(sum_MSE/num_train))
    }
}
