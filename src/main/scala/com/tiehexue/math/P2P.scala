package com.tiehexue.math

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

object P2P extends App {

  val conf = new SparkConf().setAppName("math")
  val sc = new SparkContext(conf)

  val primesRDD = sc.textFile(args(0)).flatMap(_.split(',')).map(_.trim.toInt)
  val primes = sc.broadcast(primesRDD.collect())

  def _p2p(p: Int) = {
    val i = primes.value.indexOf(p)
    var j = 1

    while (primes.value(i + j) < 2 * p) {
      j += 1
    }

    j - 1
  }

  val halfMaxEven = (primes.value.last + 1) / 2

  val p2p = primesRDD.zipWithIndex.filter(_._1 <= halfMaxEven).sortBy(_._1).map{ case (x, i) =>
    s"${x},${i + 1},${_p2p(x)}"
  }

  FileUtils.deleteDirectory(new File(args(1)))
  p2p.saveAsTextFile(args(1))
}
