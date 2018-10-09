package twitter.rdd

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RsRDD {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 4) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input nodes dir> <input edges dir> <output dir> <max>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Twitter Followers Count")

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

    //    edgesFileRDD.collect().map(println)

    // Transforming  RDD to make tuples which will reduced eventually
    val reverseFileRDD = edgesFile.map(line => {
      val spilit = line.split(",")
      (spilit(1), spilit(0))
    }).filter(x => x._1.toInt < max && x._2.toInt < max)


    //    reverseFileRDD.collect().map(println)


    val twopath = reverseFileRDD.join(edgesFileRDD).map(x => (x._2._2, x._2._1))


//    twopath.collect().map(println)


    val triangle = twopath.join(edgesFileRDD).filter(x => x._2._1 == x._2._2)

    println(triangle.count() / 3)


  }
}