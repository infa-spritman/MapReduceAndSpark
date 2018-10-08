package twitter

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object DSET {

  case class TwitterEdge(followerID: Int, followedID: Int)

  case class TwitterNode(followerID: Int)

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 3) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input nodes dir> <input edges dir> <output dir>")
      System.exit(1)
    }

    // Intializing the app and setting the app name
    val conf = new SparkConf().setAppName("Twitter Followers Count DSET")

    //    conf.set("spark.eventLog.enabled","true")

    // Setting the spark Session
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._


    // Intializing Spark Context
    //    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    // Creating Dataset from nodes file
    val nodeSchemaString = "followerID"

    val nodeFields = nodeSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = false))

    val nodeSchema = StructType(nodeFields)

    val nodesFile = sparkSession.read.schema(nodeSchema).csv(args(0)).as[TwitterNode]

    val edgeSchemaString = "followerID followedID"

    val edgeFields = edgeSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = false))

    val edgeSchema = StructType(edgeFields)

    val edgesFile = sparkSession.read.schema(edgeSchema).csv(args(1)).as[TwitterEdge]



    // Transforming  Dataset to make tuples which will reduced eventually
    val edgesCount = edgesFile.map(line => (line.followedID, 1))

    val nodesCount = nodesFile.map(word => (word.followerID, 0))


     //Joining two Dataset, then reducing it based on key
    val unionDataset = edgesCount.union(nodesCount)

    val finalDataset = unionDataset.groupBy("_1").count()

    finalDataset.coalesce(1).write.csv(args(2))

    logger.info("Explain Info:\n" + finalDataset.explain(true))
  }
}