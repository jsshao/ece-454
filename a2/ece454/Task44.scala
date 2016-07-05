package ece454

import org.apache.spark.{SparkContext, SparkConf}
import Math.sqrt

object Task4 {
    def lineToRatings(line: String): (String, Array[Double]) = {
        val array = line.split(",")
        val movie = array(0)
        val ratings = array.drop(1).map {
            x => if (x != "") (x.toInt) else 0
        }
        val numRatings = ratings.filter(_ != 0).size
        val avg = ratings.sum.toDouble/numRatings
        val userRatings = ratings.map {
            x => if (x != 0) x else avg
        }
        return (movie, userRatings)
    }

    def dotProduct(as: Array[Double], bs: Array[Double]): Double = {
        (for ((a, b) <- as zip bs) yield a * b) sum
    }

    def pairToSimilarity(a: (String, Array[Double]), b: (String, Array[Double])): String = {
        val aArr = a._2
        val bArr = b._2
        val numerator = dotProduct(aArr, bArr)
        val aNorm = sqrt(dotProduct(aArr, aArr))
        val bNorm = sqrt(dotProduct(bArr, bArr))
        "%s,%s,%1.2f".format(a._1, b._1, numerator/(aNorm*bNorm))
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Spark Task4 Application")
        val sc = new SparkContext(conf)

        val textFile = sc.textFile(args(0))

        val ratings = textFile.map(line => lineToRatings(line))
        val moviePair = ratings
            .cartesian(ratings)
            .filter(pair => pair._1._1 < pair._2._1)
        val similarities = moviePair
            .map(pair => pairToSimilarity(pair._1, pair._2))
        similarities.saveAsTextFile(args(1))
    }
}
