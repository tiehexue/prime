package com.tiehexue.math

import java.io.PrintWriter

import scala.collection.mutable
import scala.io.Source

object PrimeSum extends App {

  val primes = Source.fromInputStream(getClass.getResourceAsStream("/primes.txt")).getLines().flatMap(_.split(',')).map(_.trim.toInt).toList

  val writer1 = new PrintWriter("./summary.txt")
  val writer2 = new PrintWriter("./details.txt")

  val global = mutable.TreeSet[Int](6, 8, 10)

  (3 to primes.length).foreach { len =>
    val p = primes(len - 1)

    writer2.print(s"${len}: ")

    primes.take(len - 1).par.foreach { x =>
      global.add(x + p)
      writer2.print(s"${x+p}=${x},${p};")
    }

    global.add(2 * p)
    writer2.print(s"${p+p}=${p},${p};")
    writer2.println()

    val out = s"${len} : ${global.size} : ${global.lastKey / 2 - 2 - global.size} : ${global.lastKey}"

    writer1.println(out)
  }

  writer1.close()

  writer2.close()
}
