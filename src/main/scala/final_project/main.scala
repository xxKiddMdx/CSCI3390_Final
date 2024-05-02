package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object Main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GraphMatching")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    if (args.length == 0) {
      println("Usage: GraphMatching option = {compute, verify}")
      sys.exit(1)
    }

    if (args(0) == "compute") {
      if (args.length != 3) {
        println("Usage: GraphMatching compute graph_path output_path")
        sys.exit(1)
      }

      val startTimeMillis = System.currentTimeMillis()

      val edgesRDD = sc.textFile(args(1)).map(line => {
        val x = line.split(",")
        Edge(x(0).toLong, x(1).toLong, 1)
      })

      val graph = Graph(
        sc.emptyRDD[(Long, Int)], // Placeholder for vertices
        edgesRDD,
        0,  // Default vertex attribute
        StorageLevel.MEMORY_AND_DISK,  // Edge storage level
        StorageLevel.MEMORY_AND_DISK  // Vertex storage level
      )

      val matching = maximalMatching(graph)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println(s"Luby's Matching Algorithm completed in $durationSeconds s.")

      saveMatching(matching, args(2))
    } else if (args(0) == "verify") {
      if (args.length != 3) {
        println("Usage: GraphMatching verify graph_path matching_path")
        sys.exit(1)
      }

      val edgesRDD = sc.textFile(args(1)).map(line => {
        val x = line.split(",")
        Edge(x(0).toLong, x(1).toLong, 1)
      })

      val verticesRDD = sc.textFile(args(2)).map(line => {
        val x = line.split(",")
        (x(0).toLong, x(1).toInt)
      })

      val graph = Graph(
        verticesRDD,
        edgesRDD,
        0,  // Default vertex attribute
        StorageLevel.MEMORY_AND_DISK,  // Edge storage level
        StorageLevel.MEMORY_AND_DISK  // Vertex storage level
      )

      val isMatching = verifyMatching(graph)

      if (isMatching)
        println("Yes")
      else
        println("No")
    }
  }

  def maximalMatching(graph: Graph[Int, Int]): List[(Long, Long)] = {
    var matchedVertices = Set[Long]()
    val edges = graph.edges.collect().toList

    val matching = edges.filter { edge =>
      val u = edge.srcId
      val v = edge.dstId

      if (!matchedVertices.contains(u) && !matchedVertices.contains(v)) {
        matchedVertices += u
        matchedVertices += v
        true
      } else {
        false
      }
    }

    // Return the matching
    return matching.map(edge => (edge.srcId, edge.dstId))
  }

  def saveMatching(matching: List[(Long, Long)], outputFile: String) {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.createDataFrame(matching).toDF("u", "v")

    df.coalesce(1).write.format("csv").mode("overwrite").save(outputFile)
  }

  def verifyMatching(graph: Graph[Int, Int]): Boolean = {
  val independent = graph.triplets.flatMap { triplet =>
    if (triplet.srcAttr == 1 && triplet.dstAttr == 1) {
      Some((triplet.srcId, triplet.dstId))
    } else None
  }.count() == 0

  val maximal = graph.vertices.leftOuterJoin(graph.aggregateMessages[Int](
    triplet => {
      if (triplet.srcAttr == 1) triplet.sendToDst(1)
      if (triplet.dstAttr == 1) triplet.sendToSrc(1)
    },
    (a, b) => a + b
  )).map {
    case (id, (label, Some(count))) => label == 1 || count > 0
    case (id, (label, None)) => label == 1
  }.reduce(_ && _)

  // Return the combined result
  return independent && maximal
}
}