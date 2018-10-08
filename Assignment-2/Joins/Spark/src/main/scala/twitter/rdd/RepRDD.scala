package twitter.rdd

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RepRDD {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 4) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input nodes dir> <input edges dir> <output dir> <max>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Twitter Followers Count").setMaster("local[2]")

    // Intializing Spark Context
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val max = args(3).toInt

    // Creating Pair RDD from edges file
    val edgesFile = sc.textFile(args(1))

    val edgesFileRDD = edgesFile.map(line => {
      val spilit = line.split(",")
      (spilit(0), spilit(1))
    }).filter(x => x._1.toInt < max && x._2.toInt < max)

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val localRDD = edgesFileRDD.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val broadcastVar = sc.broadcast(localRDD.collectAsMap())

    //    val localRDD = edgesFileRDD.coalesce(1).collectAsMap()
    //
    //    val broadcastVar = sc.broadcast(localRDD)
    //
    ////        edgesFileRDD.collect().map(println)
    //    broadcastVar.value.map(println)
    //
    //
    //
    // Transforming  RDD to make tuples which will reduced eventually
    val reverseFileRDD = edgesFile.map(line => {
      val spilit = line.split(",")
      (spilit(1), spilit(0))
    }).filter(x => x._1.toInt < max && x._2.toInt < max)


    //        reverseFileRDD.collect().map(println)
    val twopath = reverseFileRDD.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) =>
          broadcastVar.value.get(k) match {
            case None => Seq.empty[(String, String)]
            case Some(v2) => {
              var emptySeq = Seq.empty[(String, String)]
              v2.foreach(v3 => emptySeq = emptySeq :+ (v3, v1))
              emptySeq
            }
          }
      }
    }, preservesPartitioning = true)

    twopath.collect().map(println)

    val triangle = twopath.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) =>
          broadcastVar.value.get(k) match {
            case None => Seq.empty[(String, String)]
            case Some(v2) => if (v2.contains(v1)) Seq((v1, v1)) else Seq.empty[(String, String)]
          }
      }
    }, preservesPartitioning = true)



    //    triangle.collect().map(println)

    //    val triangle = twopath.join(edgesFileRDD)

    println(triangle.count() / 3)


  }
}