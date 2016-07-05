package ece454

import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def highest(line: String): Array[(Int, Int)] = {
      line.split(",")
        .zipWithIndex
        .drop(1)
        .filter(_._1 != "")
        .map(pair => (pair._2, pair._1.toInt))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Task3 Application")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    textFile.flatMap(line => highest(line))
        .groupByKey()
        .map(v => (v._1, "%1.1f".format(v._2.sum.toDouble/v._2.size)))
        .saveAsTextFile(args(1))
  }
}
