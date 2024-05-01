package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.util.Random

case class MatchedEdge(vertex1: Long, vertex2: Long)

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  // Greedy Matching Algorithm
  def greedyMatching(graphInput: Graph[Int, Int]): RDD[MatchedEdge] = {
    var activeEdges: RDD[Edge[Int]] = graphInput.edges
    var selectedEdges = Set[Edge[Int]]()

    def isAnyEdgeActive(edges: RDD[Edge[Int]]): Boolean = edges.count() > 0

    while (isAnyEdgeActive(activeEdges)) {
      // Select an edge arbitrarily (greedy choice)
      val selectedEdge = activeEdges.take(1).head

      // Add the selected edge to the matching set
      selectedEdges += selectedEdge

      // Find the vertices of the selected edge
      val selectedEdgeVertices = Set(selectedEdge.srcId, selectedEdge.dstId)

      // Remove all edges that share vertices with the selected edge
      activeEdges = activeEdges.filter(e => !selectedEdgeVertices.contains(e.srcId) && !selectedEdgeVertices.contains(e.dstId))
    }

    val matchedEdges: RDD[MatchedEdge] = graphInput.edges.sparkContext.parallelize(selectedEdges.toSeq).map(e => MatchedEdge(e.srcId, e.dstId))
    matchedEdges
  }

  def lineToCanonicalEdge(line: String): Edge[Int] = {
    val x = line.split(",")
    if (x(0).toLong < x(1).toLong)
      Edge(x(0).toLong, x(1).toLong, 1)
    else
      Edge(x(1).toLong, x(0).toLong, 1)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Greedy Matching and Verifier")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    if (args.length < 2) {
      println("Usage: MainApp <command> <graph_path> [output_path]")
      sys.exit(1)
    }

    val operation = args(0)
    operation match {
      case "compute" if args.length == 3 =>
        val startTimeMillis = System.currentTimeMillis()

        val edges = sc.textFile(args(1)).map { line =>
          val parts = line.split(",")
          Edge(parts(0).toLong, parts(1).toLong, 1)
        }
        val graph = Graph.fromEdges[Int, Int](edges, defaultValue = 0)
        val matchedEdges = greedyMatching(graph)

        val endTimeMillis = System.currentTimeMillis()
        val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
        println("==================================")
        println("Greedy Matching algorithm completed in " + durationSeconds + "s.")
        println("==================================")

        // Save the results
        val matchedEdgesDF = matchedEdges.toDF("vertex1", "vertex2")
        matchedEdgesDF.coalesce(1).write.format("csv").mode("overwrite").save(args(2))

      case "verify" if args.length == 3 =>
        val graphEdges = sc.textFile(args(1)).map(lineToCanonicalEdge)
        val matchedEdges = sc.textFile(args(2)).map(lineToCanonicalEdge)

        if (matchedEdges.distinct().count() != matchedEdges.count()) {
          println("The matched edges contain duplications of an edge.")
          sys.exit(1)
        }

        if (matchedEdges.intersection(graphEdges).count() != matchedEdges.count()) {
          println("The matched edges are not a subset of the input graph.")
          sys.exit(1)
        }

        val matchedGraph = Graph.fromEdges[Int, Int](matchedEdges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
        if (matchedGraph.ops.degrees.aggregate(0)((x, v) => scala.math.max(x, v._2), (x, y) => scala.math.max(x, y)) >= 2) {
          println("The matched edges do not form a matching.")
          sys.exit(1)
        }

        println("The matched edges form a matching of size: " + matchedEdges.count())

      case _ =>
        println("Invalid command or number of arguments")
        sys.exit(1)
    }
  }
}
