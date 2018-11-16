package com.tiehexue.math

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._

import scala.io.Source
object MissingLen {
  val conf = new SparkConf().setAppName("math")
  val sc = new SparkContext(conf)

  case class Sum(sum: Int, first: Int, second: Int)
  case class Line(len: Int, sums: Array[Sum])

  def main(args: Array[String]) = {

    val strs = sc.textFile(args(0))
    val primes = sc.broadcast(Source.fromInputStream(getClass.getResourceAsStream("/primes.txt")).getLines().flatMap(_.split(',')).map(_.toInt).toList)

    val pattern = "([0-9]+)=([0-9]+),([0-9]+)".r

    val lines = strs.flatMap { x =>
      val vals = x.split(": ")
      val len = vals(0).toInt
      val str = vals(1).split(";")

      str.map { s =>
        val pattern(sum, first, second) = s
        sum.toInt -> (len, primes.value(len-1))
      }.distinct
    }

    // lines.reduceByKey((x1, x2) => scala.math.min(x1, x2))

  }
}
