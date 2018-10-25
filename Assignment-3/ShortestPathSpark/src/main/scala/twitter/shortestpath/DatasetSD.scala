package twitter.shortestpath

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object DatasetSD {

  case class Graph(v1: Int, v2: Seq[Int])

  case class Distance(v1: Int, d1: Int, d2: Int)

  def extractVertices(adjList: Seq[Int], joinData: Distance): TraversableOnce[Distance] = {


    var distanceMapToRun = Seq.empty[Distance]


    var currentDistanceToFromSource1ToN = joinData.d1
    currentDistanceToFromSource1ToN = if (currentDistanceToFromSource1ToN == Int.MaxValue) currentDistanceToFromSource1ToN else currentDistanceToFromSource1ToN + 1
    var currentDistanceToFromSource2ToN = joinData.d2
    currentDistanceToFromSource2ToN = if (currentDistanceToFromSource2ToN == Int.MaxValue) currentDistanceToFromSource2ToN else currentDistanceToFromSource2ToN + 1

    for (i <- adjList.indices) {
      if (i == 0)
        distanceMapToRun = distanceMapToRun :+ DatasetSD.Distance(adjList(i), joinData.d1, joinData.d2)
      else
        distanceMapToRun = distanceMapToRun :+ DatasetSD.Distance(adjList(i), currentDistanceToFromSource1ToN, currentDistanceToFromSource2ToN)

    }


    distanceMapToRun
  }

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 2) {
      logger.error("Usage:\n DS adj k")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Page Rank RDD").setMaster("local[*]")

    // Intializing Spark Context
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    val adjcencyFile = sc.textFile(args(0))

    val sources = Seq(5, 8)

    val graphAdj = adjcencyFile.map(line => {
      val spilit = line.split("-")
      val vertexItself = spilit(0).toInt
      val adjSpilit = spilit(1).substring(1, spilit(1).length - 1).split(":")

      if (adjSpilit.length == 1 && adjSpilit(0).equals("")) {
        Graph(vertexItself, Seq[Int](vertexItself))
      }
      else {
        Graph(vertexItself, vertexItself +: adjSpilit.map(_.toInt).toSeq)
      }
    })

    val graphAdjDS = sparkSession.createDataset(graphAdj)


    graphAdjDS.persist()


    var distances = graphAdjDS.map(adj => if (adj.v1 == sources(0)) Distance(adj.v1, 0, Int.MaxValue)
    else if (adj.v1 == sources(1)) Distance(adj.v1, Int.MaxValue, 0)
    else Distance(adj.v1, Int.MaxValue, Int.MaxValue))



    // Function extractVertices returns each vertex id m in n’s adjacency list as (m, distance(n)+w),
    // where w is the weight edge (n, m). It also returns n itself as (n, distance(n))

    var isEqual = false
    var interatonCount = 1
    while (!isEqual && interatonCount < 11) {
      // Use Accumulator instead to determine when last iteration is reached
      var temp1 = graphAdjDS.joinWith(distances, graphAdjDS("v1") === distances("v1"))

      var temp2 = temp1
        .flatMap(x => extractVertices(x._1.v2, x._2))

      var newDistances = temp2.groupBy(temp2("v1"))
        .agg(min("d1").alias("d1"),
          min("d2").alias("d2")).map(x => Distance(x.getInt(0), x.getInt(1), x.getInt(2)))


      isEqual = newDistances.joinWith(distances, newDistances("v1") === distances("v1"))
        .map(x => x._1.d1 == x._2.d1 && x._1.d2 == x._2.d2).reduce(_ && _)


      distances = newDistances
      interatonCount = interatonCount + 1
    }

    distances.collect().foreach(println)



    logger.info("Diameter: " + interatonCount - 2)


  }

}
