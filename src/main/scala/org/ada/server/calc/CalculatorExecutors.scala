package org.ada.server.calc

import org.ada.server.calc.impl._
import org.ada.server.calc.CalculatorExecutor._
import org.ada.server.calc.impl.MultiCountDistinctCalc.MultiCountDistinctCalcTypePack
import org.ada.server.calc.impl.UniqueDistributionCountsCalc.UniqueDistributionCountsCalcTypePack

import scala.reflect.runtime.universe._

trait CalculatorExecutors {

  // Unique distribution counts

  def uniqueDistributionCountsExec[T](
    implicit inputTypeTag: TypeTag[UniqueDistributionCountsCalcTypePack[T]#IN]
  ) = withSingle(UniqueDistributionCountsCalc[T])

  def uniqueDistributionCountsSeqExec[T](
    implicit inputTypeTag: TypeTag[UniqueDistributionCountsCalcTypePack[T]#IN]
  ) = withSeq(UniqueDistributionCountsCalc[T])

  def groupUniqueDistributionCountsExec[G, T](
    implicit inputTypeTag: TypeTag[GroupUniqueDistributionCountsCalcTypePack[G, T]#IN]
  ) = with2Tuple(GroupUniqueDistributionCountsCalc[G, T])

  def groupUniqueDistributionCountsSeqExec[G, T](
    implicit inputTypeTag: TypeTag[GroupUniqueDistributionCountsCalcTypePack[G, T]#IN]
  ) = withSeq(GroupUniqueDistributionCountsCalc[G, T])

  // Numeric distribution counts

  def numericDistributionCountsExec =
    withSingle(NumericDistributionCountsCalc.apply)

  def numericDistributionCountsSeqExec =
    withSeq(NumericDistributionCountsCalc.apply)

  def groupNumericDistributionCountsExec[G](
    implicit inputTypeTag: TypeTag[GroupNumericDistributionCountsCalcTypePack[G]#IN]
  ) = with2Tuple(GroupNumericDistributionCountsCalc[G])

  def groupNumericDistributionCountsSeqExec[G](
    implicit inputTypeTag: TypeTag[GroupNumericDistributionCountsCalcTypePack[G]#IN]
  ) = withSeq(GroupNumericDistributionCountsCalc[G])

  // Cumulative ordered counts

  def cumulativeOrderedCountsExec[T: Ordering](
    implicit inputTypeTag: TypeTag[CumulativeOrderedCountsCalcTypePack[T]#IN]
  ) = withSingle(CumulativeOrderedCountsCalc[T])

  def cumulativeOrderedCountsSeqExec[T: Ordering](
    implicit inputTypeTag: TypeTag[CumulativeOrderedCountsCalcTypePack[T]#IN]
  ) = withSeq(CumulativeOrderedCountsCalc[T])

  def cumulativeOrderedCountsAnyExec =
    CumulativeOrderedCountsAnyExec.withSingle

  def cumulativeOrderedCountsAnySeqExec =
    CumulativeOrderedCountsAnyExec.withSeq

  def groupCumulativeOrderedCountsExec[G, T: Ordering](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, T]#IN]
  ) = with2Tuple(GroupCumulativeOrderedCountsCalc[G, T])

  def groupCumulativeOrderedCountsSeqExec[G, T: Ordering](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, T]#IN]
  ) = withSeq(GroupCumulativeOrderedCountsCalc[G, T])

  def groupCumulativeOrderedCountsAnyExec[G](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, Any]#IN]
  ) = GroupCumulativeOrderedCountsAnyExec.with2Tuple[G]

  def groupCumulativeOrderedCountsAnySeqExec[G](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, Any]#IN]
  ) = GroupCumulativeOrderedCountsAnyExec.withSeq[G]

  // Cumulative numeric bin counts

  def cumulativeNumericBinCountsExec =
    withSingle(CumulativeNumericBinCountsCalc.apply)

  def cumulativeNumericBinCountsSeqExec =
    withSeq(CumulativeNumericBinCountsCalc.apply)

  def groupCumulativeNumericBinCountsExec[G](
    implicit inputTypeTag: TypeTag[GroupCumulativeNumericBinCountsCalcTypePack[G]#IN]
  ) = with2Tuple(GroupCumulativeNumericBinCountsCalc.apply[G])

  def groupCumulativeNumericBinCountsSeqExec[G](
    implicit inputTypeTag: TypeTag[GroupCumulativeNumericBinCountsCalcTypePack[G]#IN]
  ) = withSeq(GroupCumulativeNumericBinCountsCalc.apply[G])

  // Basic stats

  def basicStatsExec =
    withSingle(BasicStatsCalc)

  def basicStatsSeqExec =
    withSeq(BasicStatsCalc)

  def multiBasicStatsSeqExec =
    withSeq(MultiBasicStatsCalc)

  // Count distinct

  def countDistinctExec[T](
    implicit inputTypeTag: TypeTag[CountDistinctCalcTypePack[T]#IN]
  ) =
    withSingle(CountDistinctCalc[T])

  def countDistinctSeqExec[T](
    implicit inputTypeTag: TypeTag[CountDistinctCalcTypePack[T]#IN]
  ) =
    withSeq(CountDistinctCalc[T])

  def multiCountDistinctSeqExec[T](
    implicit inputTypeTag: TypeTag[MultiCountDistinctCalcTypePack[T]#IN]
  ) =
    withSeq(MultiCountDistinctCalc[T])

  // Standardization

  def standardizationExec =
    withSeq(StandardizationCalc)

  // Tuples

  def tupleExec[A, B](
    implicit inputTypeTag: TypeTag[TupleCalcTypePack[A, B]#IN]
  ) =
    with2Tuple(TupleCalc.apply[A, B])

  def tupleSeqExec[A, B](
    implicit inputTypeTag: TypeTag[TupleCalcTypePack[A, B]#IN]
  ) =
    withSeq(TupleCalc.apply[A, B])

  def tuple3Exec[A, B, C](
    implicit inputTypeTag: TypeTag[Tuple3CalcTypePack[A, B, C]#IN]
  ) =
    with3Tuple(Tuple3Calc.apply[A, B, C])

  def tuple3SeqExec[A, B, C](
    implicit inputTypeTag: TypeTag[Tuple3CalcTypePack[A, B, C]#IN]
  ) =
    withSeq(Tuple3Calc.apply[A, B, C])

  def uniqueTupleExec[A, B](
    implicit inputTypeTag: TypeTag[TupleCalcTypePack[A, B]#IN]
  ) =
    with2Tuple(UniqueTupleCalc.apply[A, B])

  def uniqueTupleSeqExec[A, B](
    implicit inputTypeTag: TypeTag[TupleCalcTypePack[A, B]#IN]
  ) =
    withSeq(UniqueTupleCalc.apply[A, B])

  def uniqueTuple3Exec[A, B, C](
    implicit inputTypeTag: TypeTag[Tuple3CalcTypePack[A, B, C]#IN]
  ) =
    with2Tuple(UniqueTuple3Calc.apply[A, B, C])

  def uniqueTuple3SeqExec[A, B, C](
    implicit inputTypeTag: TypeTag[Tuple3CalcTypePack[A, B, C]#IN]
  ) =
    withSeq(UniqueTuple3Calc.apply[A, B, C])

  def groupTupleExec[G, A, B](
    implicit inputTypeTag: TypeTag[GroupTupleCalcTypePack[G, A, B]#IN]
  ) =
    with3Tuple(GroupTupleCalc.apply[G, A, B])

  def groupTupleSeqExec[G, A, B](
    implicit inputTypeTag: TypeTag[GroupTupleCalcTypePack[G, A, B]#IN]
  ) =
    withSeq(GroupTupleCalc.apply[G, A, B])

  def groupUniqueTupleExec[G, A, B](
    implicit inputTypeTag: TypeTag[GroupTupleCalcTypePack[G, A, B]#IN]
  ) =
    with3Tuple(GroupUniqueTupleCalc.apply[G, A, B])

  def groupUniqueTupleSeqExec[G, A, B](
    implicit inputTypeTag: TypeTag[GroupTupleCalcTypePack[G, A, B]#IN]
  ) =
    withSeq(GroupUniqueTupleCalc.apply[G, A, B])

  // Quartiles

  def quartilesExec[T: Ordering](
    implicit inputTypeTag: TypeTag[QuartilesCalcTypePack[T]#IN]
  ) = withSingle(QuartilesCalc[T])

  def quartilesSeqExec[T: Ordering](
    implicit inputTypeTag: TypeTag[QuartilesCalcTypePack[T]#IN]
  ) = withSeq(QuartilesCalc[T])

  def quartilesAnyExec =
    QuartilesAnyExec.withSingle

  def quartilesAnySeqExec =
    QuartilesAnyExec.withSeq

  def groupQuartilesExec[G, T: Ordering](
    implicit inputTypeTag: TypeTag[GroupQuartilesCalcTypePack[G, T]#IN]
  ) = withSingle(GroupQuartilesCalc[G, T])

  def groupQuartilesSeqExec[G, T: Ordering](
    implicit inputTypeTag: TypeTag[GroupQuartilesCalcTypePack[G, T]#IN]
  ) = withSeq(GroupQuartilesCalc[G, T])

  def groupQuartilesAnyExec[G](
    implicit groupTypeTag: TypeTag[G]
  ) = GroupQuartilesAnyExec.withSingle[G]

  def groupQuartilesAnySeqExec[G](
    implicit groupTypeTag: TypeTag[G]
  ) = GroupQuartilesAnyExec.withSeq[G]

  // Pearson correlation

  def pearsonCorrelationExec =
    withSeq(PearsonCorrelationCalc)

  def pearsonCorrelationAllDefinedExec =
    withSeq(AllDefinedPearsonCorrelationCalc)

  // Matthews (binary class) correlation

  def matthewsBinaryClassCorrelationExec =
    withSeq(MatthewsBinaryClassCorrelationCalc)

  // Euclidean distance

  def euclideanDistanceExec =
    withSeq(EuclideanDistanceCalc)

  def euclideanDistanceAllDefinedExec =
    withSeq(AllDefinedEuclideanDistanceCalc)

  // Seq bin aggregation

  def seqBinMeanExec =
    withSeq(SeqBinMeanCalc.apply)

  def seqBinMeanAllDefinedExec =
    withSeq(AllDefinedSeqBinMeanCalc.apply)

  def seqBinMaxExec =
    withSeq(SeqBinMaxCalc.apply)

  def seqBinMaxAllDefinedExec =
    withSeq(AllDefinedSeqBinMaxCalc.apply)

  def seqBinMinExec =
    withSeq(SeqBinMinCalc.apply)

  def seqBinMinAllDefinedExec =
    withSeq(AllDefinedSeqBinMinCalc.apply)

  def seqBinVarianceExec =
    withSeq(SeqBinVarianceCalc.apply)

  def seqBinVarianceAllDefinedExec =
    withSeq(AllDefinedSeqBinVarianceCalc.apply)

  def seqBinCountExec =
    withSeq(SeqBinCountCalc.apply)

  def seqBinCountAllDefinedExec =
    withSeq(AllDefinedSeqBinCountCalc.apply)

  // Independence Tests

  def chiSquareTestExec[G, T](
    implicit inputTypeTag: TypeTag[ChiSquareTestCalcTypePack[G, T]#IN]
  ) =
    with2Tuple(ChiSquareTestCalc[G, T])

  def oneWayAnovaTestExec[G](
    implicit inputTypeTag: TypeTag[OneWayAnovaTestCalcTypePack[G]#IN]
  ) =
    with2Tuple(OneWayAnovaTestCalc[G])

  def multiChiSquareTestExec[G, T](
    implicit inputTypeTag: TypeTag[MultiChiSquareTestCalcTypePack[G, T]#IN]
  ) =
    withSeq(MultiChiSquareTestCalc[G, T])

  def nullExcludedMultiChiSquareTestExec[G, T](
    implicit inputTypeTag: TypeTag[NullExcludedMultiChiSquareTestCalcTypePack[G, T]#IN]
  ) =
    withSeq(NullExcludedMultiChiSquareTestCalc[G, T])

  def multiOneWayAnovaTestExec[G](
    implicit inputTypeTag: TypeTag[MultiOneWayAnovaTestCalcTypePack[G]#IN]
  ) =
    withSeq(MultiOneWayAnovaTestCalc[G])

  def nullExcludedMultiOneWayAnovaTestExec[G](
    implicit inputTypeTag: TypeTag[NullExcludedMultiOneWayAnovaTestCalcTypePack[G]#IN]
  ) =
    withSeq(NullExcludedMultiOneWayAnovaTestCalc[G])
}