package twitter.pagerank

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RddPR {


  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 2) {
      logger.error("Usage:\nPageRank <K-number-of-chains>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Page Rank - Time").setMaster("local[*]")

    // Intializing Spark Context
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val k: Int = args(0).toInt

    var graph: Seq[(Int, Int)] = Seq.empty[(Int, Int)]
    val volume: Double = k * k
    val intialPR: Double = 1.0 / volume

    for {i <- 1 to k * k
         if i % k == 1 || k == 1} {
      for (j <- 0 to k - 2) {
        graph = graph :+ (i + j, i + j + 1)
      }
      graph = graph :+ (i + k - 1, 0)
    }

    val graphRDD = sc.parallelize(graph, 2).partitionBy(new HashPartitioner(k))

    // Some code here to make sure that graph has a Partitioner. This is needed for avoiding shuffling in the join below.
    // Tell Spark to try and keep this pair RDD around in memory for efficient re-use
    graphRDD.persist()

    // Create the initial PageRanks, using the page count |V|, which can be passed through the context.
    // Function mapValues ensures that the same Partitioner is used as for the graph RDD.
    var ranks = graphRDD.mapValues(x => intialPR).union(sc.parallelize(Seq((0, 0.0))))

    // Function extractVertices returns each vertex id m in n’s adjacency list as (m, n’s PageRank / number of n’s outlinks).
    var sumSeq = Seq.empty[Double]
    for (iterationCount <- 1 to 10) {
      // Use Accumulator instead to determine when last iteration is reached
      var temp = graphRDD.join(ranks).map(x => (x._2._1, x._2._2))

      var temp2 = temp.reduceByKey(_ + _)

      var delta = temp2.lookup(0)(0)


      var tempSeq = Seq.empty[(Int, Double)]

      for {i <- 1 to k * k
           if i % k == 1 || k == 1} {
        tempSeq = tempSeq :+ (i, delta / volume)
      }

      ranks = temp2.map(x => if (x._1 != 0) (x._1, delta / volume + x._2) else (x._1, 0.0)).union(sc.parallelize(tempSeq))
      val currentSum = ranks.aggregate(0.0)((x, y) => x + y._2, _ + _)
      sumSeq = sumSeq :+ currentSum
      logger.info("Sum " + iterationCount + " : " + currentSum)

    }


    val finalRank  = ranks.top(k)(Ordering[(Double, Int)].on(x => (x._2,-x._1)))
    logger.info("Final Sum\n" + sumSeq.toString())
    sc.parallelize(finalRank).coalesce(1, false).saveAsTextFile(args(1))

  }
}