package ece454

import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def lineSum(line: String): (Int, Int) = {
      val sum = line.split(",", -1).drop(1).foldLeft(0) {
        (acc, x) => 
          if (x != "") (acc+1)
          else acc
      }
      (0, sum)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Task2 Application")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val sum = textFile
        .map(line => lineSum(line))
        .reduceByKey(_+_, 1)
        .map(kv => kv._2)

    sum.saveAsTextFile(args(1))
  }
}
