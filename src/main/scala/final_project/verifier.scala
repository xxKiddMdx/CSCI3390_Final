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

  // Luby MIS Algorithm
def LubyMISEdge(graphInput: RDD[(Long, Long)]): RDD[(Long, Long)] = {
  // Add each edge with initial properties: random value, active status, and MIS membership
  var graph = graphInput.map { edge =>
    (edge, (new Random().nextDouble(), "active", "No"))
  }.cache()

  var activeEdgesExist = true
  var iteration = 0

  while (activeEdgesExist) {
    // Generate new random values for active edges
    graph = graph.mapValues {
      case (rand, status, isInMIS) if status == "active" =>
        (new Random().nextDouble(), status, isInMIS)
      case other =>
        other
    }

    // Determine the highest random value among each edge's neighbors
    val highestRandom = graph.flatMap {
      case ((u, v), (randomValue, status, _)) if status == "active" =>
        Seq((u, (v, randomValue)), (v, (u, randomValue)))  // Emit both directions
    }.reduceByKey((a, b) => if (a._2 > b._2) a else b)
      .map {
        case (u, (v, rand)) => ((u, v), rand)  // Transform to edge-pair keys
      }

    // Join to get each edge's own random value and the highest of its neighbors
    val markedForMIS = graph.join(highestRandom).map {
      case ((u, v), ((randomValue, status, isInMIS), maxRand)) =>
        if (status == "active" && randomValue > maxRand) {
          ((u, v), (randomValue, "inactive", "Yes"))  // Mark as part of MIS and deactivate
        } else {
          ((u, v), (randomValue, "inactive", "No"))   // Not part of MIS, deactivate
        }
    }


    // Update the graph for the next iteration
    graph = markedForMIS
    graph.cache()

    // Check if there are any active edges left
    activeEdgesExist = graph.filter {
      case (_, (_, status, _)) => status == "active"
    }.count() > 0

    iteration += 1
    println(s"Iteration $iteration: Active edges left? $activeEdgesExist")
  }

  // Collect the edges that are in the MIS
  val edgesInMIS = graph.filter {
    case (_, (_, _, isInMIS)) => isInMIS == "Yes"
  }.map(_._1)

  edgesInMIS
}

  def isAnyEdgeActive(graph: RDD[((Long, Long), (Double, String, String))]): Boolean = {
    graph.values.map { case (randomValue, status, _) => status == "active" }.reduce(_ || _)
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
        val edgesRDD = graph.edges.map(edge => (edge.srcId, edge.dstId))
        val matchedEdges = LubyMISEdge(edgesRDD)
        


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
