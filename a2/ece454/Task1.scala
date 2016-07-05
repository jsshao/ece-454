package ece454

import org.apache.spark.{SparkContext, SparkConf}

object Task1 {
  def highest(line: String): String = {
      val array = line.split(",", -1)
      val max = array.drop(1).foldLeft((-1)) {
        (result, x) => 
          if (x != "" && x.toInt > result) (x.toInt)
          else result
      }.toString
      val users = array.drop(1).zipWithIndex.filter(_._1 == max).map(_._2 + 1)
      array(0) + "," + (users mkString ",")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Task1 Application")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val ratings = textFile.map(line => highest(line)).saveAsTextFile(args(1))
  }
}
