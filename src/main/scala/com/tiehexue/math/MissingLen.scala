package com.tiehexue.math

import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}
object MissingLen {
  val conf = new SparkConf().setAppName("math")
  val sc = new SparkContext(conf)

  case class Sum(sum: Int, first: Int, second: Int)
  case class Line(len: Int, sums: Array[Sum])

  def main(args: Array[String]) = {

    val primes = sc.broadcast(sc.textFile(args(0)).flatMap(_.split(',')).map(_.trim.toInt).collect())
    println(s"******************* ${primes.value.size} ********************")

    val pattern = "([0-9]+)=([0-9]+),([0-9]+)".r

    val strs = sc.textFile(args(1))
    val lines = strs.flatMap { x =>
      val vals = x.split(": ")
      val len = vals(0).toInt
      val str = vals(1).split(";")

      str.map { s =>
        val pattern(sum, _, _) = s
        sum.toInt -> len
      }.distinct
    }

    val missing_len = lines.reduceByKey((x1, x2) => scala.math.min(x1, x2)).sortBy(_._1).map { x =>

      var i = 1
      while (2 * primes.value(i-1) <= x._1) {
        i += 1
      }

      s"${x._1},${x._2},${i}"
    }

    missing_len.saveAsTextFile(args(2))
  }
}
