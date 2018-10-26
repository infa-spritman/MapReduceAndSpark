package twitter.pagerank

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object DatasetPR {

  case class Edge(v1: Int, v2: Int)

  case class Rank(v1: Int, pr: Double)


  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 2) {
      logger.error("Usage:\nPageRank <K-number-of-chains>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Page Rank Dataset Time")

    // Intializing Spark Context
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    val k: Int = args(0).toInt

    var graph: Seq[Edge] = Seq.empty[Edge]
    val volume: Double = k * k
    val intialPR: Double = 1.0 / volume

    for {i <- 1 to k * k
         if i % k == 1 || k == 1} {
      for (j <- 0 to k - 2) {
        graph = graph :+ Edge(i + j, i + j + 1)
      }
      graph = graph :+ Edge(i + k - 1, 0)
    }

    val graphDS = sparkSession.createDataset(graph)

    //    logger.info("Schema: " + graphDS.printSchema())

    graphDS.persist()

    var ranksDS = graphDS.map(x => Rank(x.v1, intialPR)).union(Seq(Rank(0, 0.0)).toDS())
    var sumSeq = Seq.empty[Double]

    for (iterationCount <- 1 to 10) {
      // Use Accumulator instead to determine when last iteration is reached
      var temp = graphDS.joinWith(ranksDS, graphDS("v1") === ranksDS("v1"))

      var temp2 = temp.groupBy(temp("_1.v2")).agg(sum("_2.pr").alias("pr"))

      var delta = temp2.where(temp2("v2") === 0).collect()(0).getDouble(1)


      var tempSeq = Seq.empty[Rank]

      for {i <- 1 to k * k
           if i % k == 1 || k == 1} {
        tempSeq = tempSeq :+ Rank(i, delta / volume)
      }

      ranksDS = temp2.map(x => if (x.getInt(0) != 0) Rank(x.getInt(0), delta / volume + x.getDouble(1)) else Rank(x.getInt(0), 0.0)).union(tempSeq.toDS())

      var summation = ranksDS.agg(sum("pr")).collect()(0).getDouble(0)
      sumSeq = sumSeq :+ summation
    }

    val finalRank = ranksDS.sort($"pr".desc, $"v1").limit(k)

    logger.info("Final Sum\n" + sumSeq.toString())



    finalRank.write.option("header","true").csv(args(1))


  }
}