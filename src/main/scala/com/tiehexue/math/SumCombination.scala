package com.tiehexue.math

import org.apache.spark.{SparkConf, SparkContext}

object SumCombination {
  val conf = new SparkConf().setAppName("math")
  val sc = new SparkContext(conf)

  case class Sum(sum: Int, first: Int, second: Int)

  def main(args: Array[String]) = {

    val lines = sc.textFile(args(0))

    val pattern = "([0-9]+)=([0-9]+),([0-9]+)".r

    val sums = lines.flatMap { x =>
      val str = x.split(": ")(1).split(";")

      str.map { s =>
        val pattern(sum, first, second) = s
        Sum(sum.toInt, first.toInt, second.toInt)
      }
    }

    val m = sums.groupBy(_.sum).map(x => x._1 -> x._2.size)

    m.sortBy(_._1).map(x => s"${x._1},${x._2}").saveAsTextFile("hdfs://XXXX/math/sums.txt")
  }
}
