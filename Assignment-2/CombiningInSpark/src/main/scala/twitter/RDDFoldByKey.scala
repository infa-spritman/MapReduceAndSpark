package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object RDDFoldByKey {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 3) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input nodes dir> <input edges dir> <output dir>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Twitter Followers Count RDD-F")

    //    conf.set("spark.eventLog.enabled","true")

    // Intializing Spark Context
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    // Creating RDD from nodes file
    val nodesFile = sc.textFile(args(0))

    // Creating Pair RDD from edges file
    val edgesFile = sc.textFile(args(1))

    // Transforming  RDD to make tuples which will reduced eventually
    val edgesCount = edgesFile.map(line => (line.split(",")(1), 1))

    val nodesCount = nodesFile.map(word => (word, 0))

    // Creating a sequence of 2 pair RDD
    val RDDSeq = Seq(nodesCount, edgesCount)

    // Joining two RDD, then reducing it based on key
    val unionRDD = sc.union(RDDSeq)

    val bigRDD = unionRDD.foldByKey(0)(_ + _)

    // Coalesce all partitions into one and then saving it to file in desired format.
    bigRDD.coalesce(1, true).saveAsTextFile(args(2))

    logger.info("Lineage Info:\n" + bigRDD.toDebugString)
  }
}