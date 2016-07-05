package ece454

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.graphx.lib.{ConnectedComponents}

object SparkCC {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf())
    val graph = GraphLoader.edgeListFile(sc, args(0))
    val cc = graph.connectedComponents().vertices
    cc.map(f => f._1 + " " + f._2).saveAsTextFile(args(1))
  }
}
