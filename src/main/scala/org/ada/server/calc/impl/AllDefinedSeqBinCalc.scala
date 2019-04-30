package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, CalculatorTypePack}

import scala.collection.mutable
import org.incal.core.util.{GroupMapList, crossProduct}

trait AllDefinedSeqBinCalcTypePack[ACCUM, AGG] extends CalculatorTypePack {
  type IN = Seq[Double]
  type OUT = Traversable[(Seq[BigDecimal], AGG)]
  type INTER = mutable.ArraySeq[ACCUM]
  type OPT = Seq[NumericDistributionOptions]
  type FLOW_OPT = Seq[NumericDistributionFlowOptions]
  type SINK_OPT = FLOW_OPT
}

private[impl] trait AllDefinedSeqBinCalc[ACCUM, VAL, AGG] extends Calculator[AllDefinedSeqBinCalcTypePack[ACCUM, AGG]] with NumericDistributionCountsHelper {

  protected def getValue(
    values: Seq[Double]
  ): VAL

  protected def calcAgg(
    values: Traversable[VAL]
  ): AGG

  protected def initAccum: ACCUM

  protected def naAgg: AGG

  protected def updateAccum(
    accum: ACCUM,
    value: VAL
  ): ACCUM

  protected def accumToAgg(
    accum: ACCUM
  ): AGG

  override def fun(options: Seq[NumericDistributionOptions]) = { values =>
    if (values.nonEmpty) {

      val infos = options.zipWithIndex.map { case (option, index) =>
        val oneDimValues = values.map(_(index))
        val min = oneDimValues.min
        val max = oneDimValues.max

        val stepSize = calcStepSize(option.binCount, min, max, option.specialBinForMax)

        val minBg = BigDecimal(min)

        SingleDimBinInfo(minBg, max, option.binCount, stepSize)
      }

      val indecesValues = values.map { values =>
        val actualValue = getValue(values)

        val indeces = infos.zip(values).map { case (info, value) =>
          calcBucketIndex(info.stepSize, info.binCount, info.min, info.max)(value)
        }

        (indeces, actualValue)
      }

      val indecesAggMap = indecesValues.toGroupMap.map { case (indeces, values) =>
        (indeces, calcAgg(values))
      }

      val allIndeces = crossProduct(infos.map { info => (0 until info.binCount)}.toList )

      allIndeces.map { indeces =>
        val seqIndeces = indeces.toSeq
        val agg = indecesAggMap.get(seqIndeces).getOrElse(naAgg)

        val values = seqIndeces.zip(infos).map { case (index, info) => info.min + (index * info.stepSize) }
        (values, agg)
      }
    } else
      Seq[(Seq[BigDecimal], AGG)]()
  }

  override def flow(options: FLOW_OPT) = {
    val infos = options.map { option =>
      val stepSize = calcStepSize(option.binCount, option.min, option.max, option.specialBinForMax)

      val minBg = BigDecimal(option.min)

      SingleDimBinInfo(minBg, option.max, option.binCount, stepSize)
    }

    val gridSize = options.map(_.binCount).fold(1){_*_}

    Flow[IN].fold[INTER](
      mutable.ArraySeq(Seq.fill(gridSize)(initAccum) :_*)
    ) { case (acums, values) =>

      val combinedIndex = infos.zip(values).foldLeft(0) { case (cumIndex, (info, value)) =>
        val index = calcBucketIndex(
          info.stepSize, info.binCount, info.min, info.max)(value
        )

        (cumIndex * info.binCount) + index
      }

      val actualValue = getValue(values)

      val accum = acums(combinedIndex)
      acums.update(combinedIndex, updateAccum(accum, actualValue))

      acums
    }
  }

  override def postFlow(options: SINK_OPT) = { accums =>
    val infos = options.map { option =>
      val stepSize = calcStepSize(option.binCount, option.min, option.max, option.specialBinForMax)

      val minBg = BigDecimal(option.min)

      SingleDimBinInfo(minBg, option.max, option.binCount, stepSize)
    }

    val allIndeces = crossProduct(infos.map { info => (0 until info.binCount) }.toList)

    allIndeces.map { indeces =>
      val seqIndeces = indeces.toSeq

      val combinedIndex = infos.zip(seqIndeces).foldLeft(0){ case (cumIndex, (info, index)) =>
        (cumIndex * info.binCount) + index
      }
      val accum = accums(combinedIndex)

      val values = seqIndeces.zip(infos).map { case (index, info) =>
        info.min + (index * info.stepSize)
      }

      (values, accumToAgg(accum))
    }
  }
}

case class SingleDimBinInfo(
  min: BigDecimal,
  max: Double,
  binCount: Int,
  stepSize: BigDecimal
)