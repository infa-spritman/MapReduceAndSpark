package twitter.dataset

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object RSdataset {

  case class TwitterEdge(followerID: Int, followedID: Int)

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 4) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input nodes dir> <input edges dir> <output dir>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Twitter Followers Count")

    val max = args(3).toInt


    //    conf.set("spark.eventLog.enabled","true")

    // Setting the spark Session
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    import sparkSession.sql

    val edgeSchemaString = "followerID followedID"

    val edgeFields = edgeSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = false))

    val edgeSchema = StructType(edgeFields)

    val edgesDataset = sparkSession.read.schema(edgeSchema).csv(args(1)).as[TwitterEdge]

    edgesDataset.createOrReplaceTempView("edges")

    val query = "SELECT P.start, P.mid, P.end FROM (SELECT E1.followerID AS start, E1.followedID AS mid, E2.followedID AS end FROM edges AS E1, edges AS E2 " +
      s"WHERE E1.followedID = E2.followerID AND E1.followerID < $max AND E1.followedID < $max AND E2.followedID < $max) " +
      "AS P, edges AS E WHERE P.start = E.followedID AND P.end = E.followerID"

//    sql("SELECT E1.followerID AS start, E1.followedID AS mid, E2.followedID AS end FROM edges AS E1, edges AS E2 WHERE E1.followedID = E2.followerID").show()

    val triangleCount = sql(query).count()

    println("Triangle Count: " + triangleCount/3)
  }
}