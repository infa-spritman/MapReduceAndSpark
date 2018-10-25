package twitter.shortestpath

package twitter.rdd

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object RddSD {

  def extractVertices(joinData: (Seq[Int], (Int,Int))): TraversableOnce[(Int,(Int, Int))] = {
    var distanceMapToRun = Seq.empty[(Int,(Int,Int))]
    var adjList = joinData._1
    var currentDistanceToFromSource1ToN = joinData._2._1
    currentDistanceToFromSource1ToN = if(currentDistanceToFromSource1ToN == Int.MaxValue) currentDistanceToFromSource1ToN else currentDistanceToFromSource1ToN + 1
    var currentDistanceToFromSource2ToN = joinData._2._2
    currentDistanceToFromSource2ToN = if(currentDistanceToFromSource2ToN == Int.MaxValue) currentDistanceToFromSource2ToN else currentDistanceToFromSource2ToN + 1

      for (i <- adjList.indices) {
        if(i == 0)
          distanceMapToRun = distanceMapToRun :+ (adjList(i), (joinData._2._1,joinData._2._2))
        else
          distanceMapToRun = distanceMapToRun :+ (adjList(i), (currentDistanceToFromSource1ToN,currentDistanceToFromSource2ToN))

      }


    distanceMapToRun
  }

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 2) {
      logger.error("Usage:\nwc.RddSD <input adj dir> <k>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("RddSD scala").setMaster("local[*]")

    // Intializing Spark Context
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    // Creating Pair RDD from edges file
    val adjcencyFile = sc.textFile(args(0))

    val sources = Seq(9,10)



    val graphRDD = adjcencyFile.map(line => {
      var spilit = line.split("-")
      var vertexItself = spilit(0).toInt
      var adjSpilit = spilit(1).substring(1, spilit(1).length-1).split(":")

      if(adjSpilit.length ==1 && adjSpilit(0).equals("")){
        (vertexItself, Seq[Int](vertexItself))
      }
      else {
        (vertexItself, vertexItself +: adjSpilit.map(_.toInt).toSeq)
      }
    }).partitionBy(new HashPartitioner(sources.length*2))

    graphRDD.persist()




    var distances = graphRDD.mapValues(adj => if(adj(0) == sources(0)) (0, Int.MaxValue)
    else if(adj(0) == sources(1)) (Int.MaxValue,0)
    else (Int.MaxValue, Int.MaxValue))



    // Function extractVertices returns each vertex id m in n’s adjacency list as (m, distance(n)+w),
    // where w is the weight edge (n, m). It also returns n itself as (n, distance(n))

    var isEqual = false
    var interatonCount = 1
    while (!isEqual && interatonCount < 11) {
      // Use Accumulator instead to determine when last iteration is reached
      var temp1 =  graphRDD.join( distances )

      var temp2 = temp1.flatMap(x => extractVertices(x._2))
      var newDistances = temp2.reduceByKey((x,y) => (Math.min(x._1,y._1), Math.min(x._2,y._2)))


      isEqual = newDistances.join(distances).map(x => x._2._1 == x._2._2).reduce(_&&_)

      distances = newDistances
      interatonCount = interatonCount + 1
      // Remember the shortest of the distances found
    }


    logger.info(interatonCount + "IterationCount")
    val source1Highest = distances.map( x => x._2._1).filter( x => x != Int.MaxValue).reduce(Math.max)
    val source2Highest = distances.map( x => x._2._2).filter( x => x != Int.MaxValue).reduce(Math.max)


    if(source1Highest.isNaN && source2Highest.isNaN)
      println("Diameter is: " + 0)
    else if(!source1Highest.isNaN && source2Highest.isNaN)
      println("Diameter is: " + source1Highest)
    else if(source1Highest.isNaN && !source2Highest.isNaN)
      println("Diameter is: " + source2Highest)
    else if(!source1Highest.isNaN && !source2Highest.isNaN)
      println("Diameter is: " + Math.max(source1Highest,source2Highest))
    else
      println("Diameter is: " + 0)




  }
}


