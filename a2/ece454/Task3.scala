package ece454

import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def highest(line: String): Array[(Int, (Int, Int))] = {
      line.split(",", -1)
        .zipWithIndex
        .drop(1)
        .filter(_._1 != "")
        .map(pair => (pair._2, (pair._1.toInt, 1)))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Task3 Application")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    textFile.flatMap(line => highest(line))
        .reduceByKey((a,b) => (a._1+b._1, a._2+b._2))
        .map(v => ("%s,%1.1f".format(v._1,v._2._1.toDouble/v._2._2)))
        .saveAsTextFile(args(1))
  }
}
