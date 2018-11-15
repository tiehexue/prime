package com.tiehexue.math

object LeastSum extends App {

  def ifOver(x: List[Int], full: List[Int], dest: List[Int]): List[Int] = {
    val s1 = x.combinations(2).map(y => y.sum).toList.distinct
    val s2 = x.map(_ * 2)
    val s = s1 ++ s2

    if (dest.forall(s.contains)) {
      x.sorted
    } else if (x.length < full.length) {
      val d = (full diff x).head
      ifOver(d :: x, full, dest)
    } else {
      List()
    }
  }

  val src = 28.to(100, 2).toList
  val full = (3 to 28).toList

  src.foreach { x =>
    val n = math.sqrt(x).toInt - 1;

    val c = (3 to x).combinations(n).toList

    val results = c.par.map { y =>
      ifOver(y.toList, full, 6.to(x, 2).toList)
    }.distinct.toList

    val minLength = results.sortWith((a, b) => a.length < b.length).head.length

    results.filter(_.length == minLength).foreach { r =>
      println(s"${x} : ${r.mkString(", ")}")
    }
  }
}
