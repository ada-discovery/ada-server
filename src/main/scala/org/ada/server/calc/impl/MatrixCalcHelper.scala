package org.ada.server.calc.impl

trait MatrixCalcHelper {

  def calcGroupSizes(n: Int, parallelism: Option[Int]) =
    parallelism.map { groupNumber =>
      val initGroupSize = n / Math.sqrt(groupNumber)

      val groupSizes = (1 to groupNumber).map { i =>
        val doubleGroupSize = (Math.sqrt(i) - Math.sqrt(i - 1)) * initGroupSize
        Math.round(doubleGroupSize).toInt
      }.filter(_ > 0)

      val sum = groupSizes.sum
      val fixedGroupSizes = if (sum < n) {
        if (groupSizes.size > 1)
          groupSizes.take(groupSizes.size - 1) :+ (groupSizes.last + (n - sum))
        else if (groupSizes.size == 1)
          Seq(groupSizes.head + (n - sum))
        else
          Seq(n)
      } else
        groupSizes

//      val newSum = fixedGroupSizes.sum
//      logger.info("Groups          : " + fixedGroupSizes.mkString(","))
//      logger.info("Sum             : " + newSum)

      fixedGroupSizes
    }.getOrElse(
      Nil
    )

  def calcStartEnds(n: Int, parallelism: Option[Int]) = {
    val parallelGroupSizes = calcGroupSizes(n, parallelism)

    val starts = parallelGroupSizes.scanLeft(0){_+_}
    parallelGroupSizes.zip(starts).map{ case (size, start) => (start, Math.min(start + size, n) - 1)}
  }
}
