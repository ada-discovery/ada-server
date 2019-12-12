package org.ada.server.services

import java.{util => ju}

import javax.inject.{Inject, Singleton}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.ActorMaterializer
import com.google.inject.ImplementedBy
import play.api.Logger
import play.api.libs.json._
import org.ada.server.calc.impl._
import breeze.linalg.{DenseMatrix, eig, eigSym}
import breeze.linalg.eigSym.EigSym
import com.jujutsu.tsne.TSneConfig
import com.jujutsu.tsne.barneshut.{BHTSne, ParallelBHTsne}
import org.ada.server.dataaccess.RepoTypes.JsonReadonlyRepo
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models._
import org.incal.core.akka.AkkaStreamUtil
import org.ada.server.calc.CalculatorExecutors
import org.ada.server.field.FieldUtil.FieldOps
import org.ada.server.dataaccess.JsonReadonlyRepoExtra._
import org.incal.core.dataaccess.{AscSort, Criterion, DescSort, NotEqualsNullCriterion}
import org.incal.core.dataaccess.Criterion.Infix
import org.incal.core.util.{GroupMapList, crossProduct}
import org.apache.commons.math3.linear.{Array2DRowRealMatrix, EigenDecomposition}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConversions._
import org.ada.server.calc.CalculatorHelper.{NoOptionsExt, NoOptionsExecExt, RunExt}
import org.ada.server.calc.impl.SeqBinCountCalc.SeqBinCountCalcTypePack
import org.ada.server.calc.impl.UniqueDistributionCountsCalc.UniqueDistributionCountsCalcTypePack

@ImplementedBy(classOf[StatsServiceImpl])
trait StatsService extends CalculatorExecutors {

  //////////////////////////////////////////////
  // Unique Counts / Distribution (From Repo) //
  //////////////////////////////////////////////

  type UniqueCount[T] = UniqueDistributionCountsCalcTypePack[T]#OUT
  type GroupUniqueCount[G, T] = GroupUniqueDistributionCountsCalcTypePack[G, T]#OUT

  def calcUniqueDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[UniqueCount[Any]]

  // grouped

  def calcGroupedUniqueDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    groupField: Field
  ): Future[GroupUniqueCount[Any, Any]]

  ///////////////////////////////////////////////
  // Numeric Counts / Distribution (From Repo) //
  ///////////////////////////////////////////////

  type NumericCount = NumericDistributionCountsCalcTypePack#OUT
  type SeqNumericCount = SeqBinCountCalcTypePack#OUT
  type GroupNumericCount[G] = GroupNumericDistributionCountsCalcTypePack[G]#OUT

  def calcNumericDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    numericBinCountOption: Option[Int]
  ): Future[NumericCount]

  def calcSeqNumericDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    fieldsAndBinCounts: Seq[(Field, Option[Int])]
  ): Future[SeqNumericCount]

  // grouped

  def calcGroupedNumericDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    groupField: Field,
    numericBinCountOption: Option[Int]
  ): Future[GroupNumericCount[Any]]

  /////////////////////////
  // Quartiles From Repo //
  /////////////////////////

  type QuartilesOut = QuartilesCalcTypePack[Any]#OUT
  type GroupQuartilesOut[G] = GroupQuartilesCalcTypePack[G, Any]#OUT

  def calcQuartilesFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[QuartilesOut]

  def calcGroupQuartilesFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    groupField: Field
  ): Future[GroupQuartilesOut[Any]]

  /////////////////////////
  // Min & Max From Repo //
  /////////////////////////

  def getMinMax[T](
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[(Option[T], Option[T])]

  def getNumericMinMax(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[(Option[Double], Option[Double])]

  /////////////////
  // Gram Matrix //
  /////////////////

  def calcGramMatrix(
    matrix: Traversable[Seq[Double]]
  ): Seq[Seq[Double]]

  def calcGramMatrix(
    source: Source[Seq[Double], _]
  ): Future[Seq[Seq[Double]]]

  ////////////////
  // Metric MDS //
  ////////////////

  def performMetricMDS(
    distanceMatrix: Traversable[Seq[Double]],
    dims: Int,
    scaleByEigenValues: Boolean
  ): Future[(Seq[Seq[Double]], Seq[Double])]

  def performMetricMDS(
    distanceMatrixSource: Source[Seq[Double], _],
    dims: Int,
    scaleByEigenValues: Boolean
  ): Future[(Seq[Seq[Double]], Seq[Double])]

  ///////////
  // t-SNE //
  ///////////

  def performTSNE(
    data: Array[Array[Double]],
    setting: TSNESetting = TSNESetting()
  ): Array[Array[Double]]

  //////////////////////////
  // Eigenvectors/values //
  /////////////////////////

  def calcEigenValuesAndVectors(
    matrix: Seq[Seq[Double]]
  ): (Seq[Double], Seq[Seq[Double]])

  def calcEigenValuesAndVectorsSymMatrixBreeze(
    matrix: Seq[Seq[Double]]
  ): (Seq[Double], Seq[Seq[Double]])

  def calcEigenValuesAndVectorsBreeze(
    matrix: Seq[Seq[Double]]
  ): (Seq[Double], Seq[Double], Seq[Seq[Double]])

  /////////////////////
  // Standardization //
  /////////////////////

  def standardize(
    inputs: Traversable[Seq[Option[Double]]],
    useSampleStd: Boolean
  ): Traversable[Seq[Option[Double]]]

  def standardizeSimple(
    source: Source[Seq[Option[Double]], _],
    useSampleStd: Boolean
  ): Future[Traversable[Seq[Option[Double]]]]

  def standardize[T](
    source: Source[(T, Seq[Option[Double]]), _],
    useSampleStd: Boolean
  ): Future[Traversable[(T, Seq[Option[Double]])]]

  ////////////////////////
  // Independence Tests //
  ////////////////////////

  def testIndependence(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean = false
  ): Future[Seq[Option[IndependenceTestResult]]]

  def testIndependenceSorted(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean = false
  ): Future[Seq[(Field, Option[IndependenceTestResult])]]

  def testIndependenceSortedJson(
    items: Traversable[JsObject],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean = false
  ): Seq[(Field, Option[IndependenceTestResult])]

  def testAnovaForMultiCriteriaSorted(
    dataRepo: JsonReadonlyRepo,
    multiCriteria: Seq[Seq[Criterion[Any]]],
    inputFields: Seq[Field]
  ): Future[Seq[(Field, Option[OneWayAnovaResult])]]

  def testChiSquareForMultiCriteriaSorted(
    dataRepo: JsonReadonlyRepo,
    multiCriteria: Seq[Seq[Criterion[Any]]],
    inputFields: Seq[Field]
  ): Future[Seq[(Field, Option[ChiSquareResult])]]

  def selectFeaturesAsAnovaChiSquare(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    inputFields: Seq[Field],
    targetField: Field,
    featuresToSelectNum: Int,
    keepUndefined: Boolean = false
  ): Future[Seq[Field]]
}

@Singleton
class StatsServiceImpl extends StatsService with OneWayAnovaHelper with ChiSquareHelper[Int, Option[Any]] {

  private implicit val ftf = FieldTypeHelper.fieldTypeFactory()
  private val defaultNumericBinCount = 20

  private val logger = Logger

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  ////////////////////////////////
  // Unique Distribution Counts //
  ////////////////////////////////

  override def calcUniqueDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[UniqueCount[Any]] = {
    val spec = field.fieldTypeSpec
    field.fieldType match {
      // repo optimized unique distribution counts can be fully utilized only for enum and boolean types for which the (distinct) values are known
      case FieldTypeId.Enum =>
        val values = spec.enumValues.map(_._1).toSeq.sorted
        calcUniqueCountsFromRepo(field.name, values, dataRepo, criteria)

      case FieldTypeId.Boolean =>
        val values = Seq(true, false)
        calcUniqueCountsFromRepo(field.name, values, dataRepo, criteria)

      case _ =>
        for {
          jsons <- dataRepo.find(criteria = criteria, projection = Seq(field.name))
        } yield {
          val fieldType = ftf(spec).asValueOf[Any]
          val values = jsons.map(json => fieldType.jsonToValue(json \ field.name))
          UniqueDistributionCountsCalc[Any].fun()(values)
        }
    }
  }

  private def calcUniqueCountsFromRepo[T](
    fieldName: String,
    values: Traversable[T],
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]]
  ): Future[Seq[(Option[T], Int)]] = {
    val countFutures = values.par.map { value =>
      val finalCriteria = criteria ++ Seq(fieldName #== value)
      dataRepo.count(finalCriteria).map { count =>
        (Some(value) : Option[T], count)
      }
    }.toList

    val findNoneCriteria = criteria ++ Seq(fieldName #=@)
    val naValueFuture = dataRepo.count(findNoneCriteria).map { count =>
      (Option.empty[T], count)
    }

    Future.sequence(countFutures ++ Seq(naValueFuture))
  }

  // grouped

  override def calcGroupedUniqueDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    groupField: Field
  ): Future[GroupUniqueCount[Any, Any]] =
    for {
      groupValues <- groupValues(dataRepo, criteria, groupField)

      seriesCounts <- {
        val groupFieldName = groupField.name
        val countFutures = groupValues.par.map { value =>
          val finalCriteria = criteria ++ Seq(groupFieldName #== value)
          calcUniqueDistributionCountsFromRepo(dataRepo, finalCriteria, field).map { counts =>
            (Some(value), counts)
          }
        }.toList

        val undefinedGroupCriteria = criteria ++ Seq(groupFieldName #=@)
        val naValueFuture = calcUniqueDistributionCountsFromRepo(dataRepo, undefinedGroupCriteria, field).map { counts =>
          (None, counts)
        }

        Future.sequence(countFutures ++ Seq(naValueFuture))
      }
    } yield
      seriesCounts

  /////////////////////////////////
  // Numeric Distribution Counts //
  /////////////////////////////////

  override def calcNumericDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    numericBinCountOption: Option[Int]
  ): Future[NumericCount] =
    createRepoCountSpec(field, numericBinCountOption).map( repoCountSpec =>
      calcNumericalCountsFromRepo(dataRepo, criteria)(repoCountSpec)
    ).getOrElse(
      Future(Nil)
    )

  private def createRepoCountSpec(
    field: Field,
    numericBinCountOption: Option[Int]
  ): Option[RepoCountSpec[_]] = {
    val fieldType = ftf(field.fieldTypeSpec)
    val numericBinCount = numericBinCountOption.getOrElse(defaultNumericBinCount)

    def createAux[T](
      toBigDecimal: T => BigDecimal,
      toRangeVal: BigDecimal => Any,
      columnForEachIntValue: Boolean
    ) = Some(
      RepoCountSpec(
        toBigDecimal, toRangeVal, field.name, fieldType.asValueOf[T], numericBinCount, columnForEachIntValue
      )
    )

    field.fieldType match {
      case FieldTypeId.Double =>
        createAux(BigDecimal(_: Double), _.toDouble, false)

      case FieldTypeId.Integer =>
        createAux(BigDecimal(_: Long), _.toDouble, true)

      case FieldTypeId.Date =>
        def convert(ms: BigDecimal) = new ju.Date(ms.setScale(0, BigDecimal.RoundingMode.CEILING).toLongExact)
        createAux({x : ju.Date => BigDecimal(x.getTime)}, convert, false)

      case _ =>
        None
    }
  }

  // grouped

  override def calcGroupedNumericDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    groupField: Field,
    numericBinCountOption: Option[Int]
  ): Future[GroupNumericCount[Any]] =
    for {
      groupVals <- groupValues(dataRepo, criteria, groupField)

      seriesCounts <- {
        val groupFieldName = groupField.name
        val countFutures = groupVals.par.map { value =>
          val finalCriteria = criteria ++ Seq(groupFieldName #== value)
          calcNumericDistributionCountsFromRepo(dataRepo, finalCriteria, field, numericBinCountOption).map { counts =>
            (Some(value), counts)
          }
        }.toList

        val undefinedGroupCriteria = criteria ++ Seq(groupFieldName #=@)
        val naValueFuture = calcNumericDistributionCountsFromRepo(dataRepo, undefinedGroupCriteria, field, numericBinCountOption).map { counts =>
          (None, counts)
        }

        Future.sequence(countFutures ++ Seq(naValueFuture))
      }
    } yield
      seriesCounts

  private def groupValues(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    groupField: Field
  ) = {
    val groupFieldSpec = groupField.fieldTypeSpec

    groupFieldSpec.fieldType match {

      case FieldTypeId.Enum =>
        val values = groupFieldSpec.enumValues.map(_._1)
        Future(values)

      case FieldTypeId.Boolean =>
        Future(Seq(true, false))

      case _ =>
        for {
          jsons <- dataRepo.find(
            criteria = criteria ++ Seq(groupField.name #!@),
            projection = Seq(groupField.name)
          )
        } yield {
          val groupFieldType = ftf(groupFieldSpec)
          jsons.flatMap(json => groupFieldType.jsonToValue(json \ groupField.name)).toSet
        }
    }
  }

  case class RepoCountSpec[T](
    toBigDecimal: T => BigDecimal,
    toRangeVal: BigDecimal => Any,
    fieldName: String,
    fieldType: FieldType[T],
    maxColumnCount: Int,
    columnForEachIntValue: Boolean,
    explMin: Option[T] = None,
    explMax: Option[T] = None
  )

  private def calcNumericalCountsFromRepo[T](
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]])(
    spec: RepoCountSpec[T]
  ): Future[NumericCount] = {
    for {
      // get min and max
      (minOption, maxOption) <- getMinMaxBigDecimal(dataRepo, criteria)(spec)

      // calc the column count and step size
      columnCountStepSizeOption = calcColumnCountStepSize(minOption, maxOption, spec.maxColumnCount, spec.columnForEachIntValue)

      // obtain the binned counts
      binnedCounts <-
        minOption.zip(columnCountStepSizeOption).headOption.map { case (min, (columnCount, stepSize)) =>
          val rangeStartAndCriteria = calcRangeStartAndCriteria(spec, min, stepSize, columnCount)

          val futures = rangeStartAndCriteria.par.map {
            case (start, rangeCriteria) => dataRepo.count(rangeCriteria ++ criteria).map((start, _))
          }

          Future.sequence(futures.toList)
        }.getOrElse(
          Future(Nil)
        )

    } yield
      binnedCounts
  }

  // seq

  override def calcSeqNumericDistributionCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    fieldsAndBinCounts: Seq[(Field, Option[Int])]
  ): Future[SeqNumericCount] = {
    val repoCountSpecs = fieldsAndBinCounts.flatMap { case (field, numericBinCount) =>
      createRepoCountSpec(field, numericBinCount).asInstanceOf[Option[RepoCountSpec[Any]]]
    }
    calcSeqNumericalCountsFromRepo(dataRepo, criteria)(repoCountSpecs)
  }

  private def calcSeqNumericalCountsFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]])(
    specs: Seq[RepoCountSpec[Any]]
  ): Future[SeqNumericCount] = {
    val notNullCriteria = specs.map(spec => NotEqualsNullCriterion(spec.fieldName))

    val specMinMaxFutures = Future.sequence(
      specs.map(spec =>
        getMinMaxBigDecimal(dataRepo, criteria ++ notNullCriteria)(spec).map(minMax => (spec, minMax))
      )
    )

    for {
      // get mines and maxes
      specMinMaxes <- specMinMaxFutures

      // calc the column counts and step sizes
      specMinColumnCountStepSizes = specMinMaxes.map { case (spec, (min, max)) =>
        calcColumnCountStepSize(min, max, spec.maxColumnCount, spec.columnForEachIntValue).map(
          (spec, min, _)
        )
      }

      // obtain the binned counts
      binnedCounts <- {
        val definedSpecMinColumnCountStepSizes = specMinColumnCountStepSizes.flatten
        if (definedSpecMinColumnCountStepSizes.size == specMinColumnCountStepSizes.size) {

          val rangeStartAndCriteria = crossProduct(
            definedSpecMinColumnCountStepSizes.map { case (spec, min, (columnCount, stepSize)) =>
              calcRangeStartAndCriteria(spec, min.get, stepSize, columnCount)
            }
          )

          val futures = rangeStartAndCriteria.par.map { rangeStartAndCriteria =>
            val rangeStartAndCriteriaSeq = rangeStartAndCriteria.toSeq
            val rangeCriteria = rangeStartAndCriteriaSeq.flatMap(_._2)
            val starts = rangeStartAndCriteriaSeq.map(_._1)
            dataRepo.count(rangeCriteria ++ criteria).map((starts, _))
          }

          Future.sequence(futures.toList)
        } else
          Future(Nil)
      }
    } yield
      binnedCounts
  }

  private def getMinMaxBigDecimal[T](
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]])(
    spec: RepoCountSpec[T]
  ): Future[(Option[BigDecimal], Option[BigDecimal])] = {
    def jsonToBigDecimalValue(jsValue: JsReadable): Option[BigDecimal] =
      spec.fieldType.jsonToValue(jsValue).map(spec.toBigDecimal)

    // future to retrieve a max value if not explicitly provided
    val maxFuture =
      if (spec.explMax.isDefined)
        Future(Some(spec.toBigDecimal(spec.explMax.get)))
      else
        dataRepo.max(spec.fieldName, criteria, true).map(
          _.flatMap(jsonToBigDecimalValue)
        )

    // future to retrieve a min value if not explicitly provided
    val minFuture =
      if (spec.explMin.isDefined)
        Future(Some(spec.toBigDecimal(spec.explMin.get)))
      else
        dataRepo.min(spec.fieldName, criteria, true).map(
          _.flatMap(jsonToBigDecimalValue)
        )

    for {
      min <- minFuture
      max <- maxFuture
    } yield
      (min, max)
  }

  private def calcColumnCountStepSize(
    minOption: Option[BigDecimal],
    maxOption: Option[BigDecimal],
    maxColumnCount: Int,
    columnForEachIntValue: Boolean
  ): Option[(Int, BigDecimal)] =
    minOption.zip(maxOption).headOption.map { case (min, max) =>
      val columnCount =
        if (columnForEachIntValue) {
          val valueCount = max - min
          Math.min(maxColumnCount, valueCount.toInt + 1)
        } else
          maxColumnCount

      val stepSize: BigDecimal = if (min == max)
        0
      else if (columnForEachIntValue && columnCount < maxColumnCount)
        (max - min) / (columnCount - 1)
      else
        (max - min) / columnCount

      (columnCount, stepSize)
    }

  private def calcRangeStartAndCriteria[T](
    spec: RepoCountSpec[T],
    min: BigDecimal,
    stepSize: BigDecimal,
    columnCount: Int
  ): Seq[(BigDecimal, Seq[Criterion[Any]])] =
    if (stepSize == 0) {
      val rangeCriteria = Seq(spec.fieldName #== spec.toRangeVal(min))
      Seq((min, rangeCriteria))
    } else {
      (0 until columnCount).map { columnIndex =>
        calcRangeStartAndCriteriaAux(spec.fieldName, min, stepSize, columnCount, columnIndex, spec.toRangeVal)
      }
    }

  private def calcRangeStartAndCriteriaAux(
    fieldName: String,
    min: BigDecimal,
    stepSize: BigDecimal,
    columnCount: Int,
    columnIndex: Int,
    toRangeVal: BigDecimal => Any
  ): (BigDecimal, Seq[Criterion[Any]]) = {
    val start = min + (columnIndex * stepSize)
    val end = min + ((columnIndex + 1) * stepSize)

    val startVal = toRangeVal(start)
    val endVal = toRangeVal(end)

    val criteria = if (columnIndex < columnCount - 1)
      Seq(fieldName #>= startVal, fieldName #< endVal)
    else
      Seq(fieldName #>= startVal, fieldName #<= endVal)
    (start, criteria)
  }

  ///////////////
  // Quartiles //
  ///////////////

  override def calcQuartilesFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[QuartilesOut] = {
    val typeSpec = field.fieldTypeSpec

    def quartiles[T: Ordering](toDouble: T => Double) =
      calcQuartilesFromRepo[T](dataRepo, criteria, field, toDouble).map(
        _.asInstanceOf[Option[Quartiles[Any]]]
      )

    typeSpec.fieldType match {
      case FieldTypeId.Double => quartiles[Double](identity)
      case FieldTypeId.Integer => quartiles[Long](_.toDouble)
      case FieldTypeId.Date => quartiles[ju.Date](_.getTime.toDouble)
      case _ => Future(None)
    }
  }

  override def calcGroupQuartilesFromRepo(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    groupField: Field
  ): Future[GroupQuartilesOut[Any]] =
    for {
      groupVals <- groupValues(dataRepo, criteria, groupField)

      seriesCounts <- {
        val groupFieldName = groupField.name
        val countFutures = groupVals.par.map { value =>
          val finalCriteria = criteria ++ Seq(groupFieldName #== value)

          calcQuartilesFromRepo(dataRepo, finalCriteria, field).map { counts =>
            (Some(value), counts)
          }
        }.toList

        val undefinedGroupCriteria = criteria ++ Seq(groupFieldName #=@)
        val naValueFuture = calcQuartilesFromRepo(dataRepo, undefinedGroupCriteria, field).map { counts =>
          (None, counts)
        }

        Future.sequence(countFutures ++ Seq(naValueFuture))
      }
    } yield
      seriesCounts

  private def calcQuartilesFromRepo[T: Ordering](
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field,
    toDouble: T => Double
  ): Future[Option[Quartiles[T]]] =
    for {
      // total length
      length <- dataRepo.count(criteria ++ Seq(field.name #!@))

      // create quartiles
      quants <-
        if (length > 0)
          createQuantilesAux(toDouble, length, dataRepo, criteria, field)
        else
          Future(None)
    } yield
      quants

  private def createQuantilesAux[T: Ordering](
    toDouble: T => Double,
    length: Int,                                            // must be non-zero
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[Option[Quartiles[T]]] = {
    val typeSpec = field.fieldTypeSpec
    val fieldType = ftf(typeSpec).asValueOf[T]

    def headResultToValue(
      results: Traversable[JsObject]
    ): Option[T] =
      results.headOption.map( json =>
        fieldType.jsonToValue(json \ field.name)
      ).flatten

    def getItem(position : Int, ascOrder: Boolean): Future[Option[T]] = {
//        fromValue.map { minValue => Seq(field.name #> minValue) }

      dataRepo.find(
        criteria = criteria ++ Seq(field.name #!@),
        projection = Seq(field.name),
        sort = Seq(if (ascOrder) AscSort(field.name) else DescSort(field.name)),
        skip = Some(Math.max(position - 1, 0)),
        limit = Some(1)
      ).map(headResultToValue)
    }

    for {
      // lower quartile
      lowerQuantileOption <- getItem(length / 4, true)

      // lower quartile less equals count
      lowerQuantileLessEqualsCountOption <-
        lowerQuantileOption match {
          case Some(lowerQuantile) =>
            dataRepo.count(
              criteria = criteria ++ Seq(field.name #<= lowerQuantile)
            ).map(Some(_))
          case None => Future(None)
        }

      //      // median
//      medianOption <- getItem(length / 2, true)

      // upper quartile
      upperQuantileOption <- getItem(length / 4, false)

      // lower and upper whiskers
      medianLowerUpperWhiskerOptions <- {
        val optionFuture = for {
          upper <- upperQuantileOption
          lower <- lowerQuantileOption
          lowerQuantileLessEqualsCount <- lowerQuantileLessEqualsCountOption
        } yield {
          val doubleUpper = toDouble(upper)
          val doubleLower = toDouble(lower)

          val iqr = doubleUpper - doubleLower

          val upperWhiskerValue = doubleUpper + 1.5 * iqr
          val lowerWhiskerValue = doubleLower - 1.5 * iqr

          val medianPos = length / 2
          val relativeMedianPos = medianPos - lowerQuantileLessEqualsCount

          val medianFuture =
            if (relativeMedianPos > 0) {
              dataRepo.find(
                criteria = criteria ++ Seq(field.name #> lower),
                projection = Seq(field.name),
                sort = Seq(AscSort(field.name)),
                skip = Some(relativeMedianPos - 1),
                limit = Some(1)
              ).map { results =>
                headResultToValue(results)
              }
            } else
              Future(Some(lower))

          val lowerWhiskerFuture =
            dataRepo.find(
              criteria = criteria ++ Seq(field.name #>= lowerWhiskerValue),
              projection = Seq(field.name),
              sort = Seq(AscSort(field.name)),
              limit = Some(1)
            ).flatMap { results =>
              headResultToValue(results) match {
                // if no value found take the first item
                case Some(value) => Future(Some(value))
                case None => getItem(1, true)
              }
            }

          val upperWhiskerFuture =
            dataRepo.find(
              criteria = criteria ++ Seq(field.name #<= upperWhiskerValue),
              projection = Seq(field.name),
              sort = Seq(DescSort(field.name)),
              limit = Some(1)
            ).flatMap { results =>
              headResultToValue(results) match {
                // if no value found take the last item
                case Some(value) => Future(Some(value))
                case None => getItem(1, false)
              }
            }

          for {
            median <- medianFuture
            lowerWhisker <- lowerWhiskerFuture
            upperWhisker <- upperWhiskerFuture
          } yield
            (median, lowerWhisker, upperWhisker)
        }
        optionFuture.getOrElse(
          Future(None, None, None)
        )
      }
    } yield {
      for {
        lowerWhisker <- medianLowerUpperWhiskerOptions._2
        lowerQuantile <- lowerQuantileOption
        median <- medianLowerUpperWhiskerOptions._1
        upperQuantile <- upperQuantileOption
        upperWhisker <- medianLowerUpperWhiskerOptions._3
      } yield {
        Quartiles(lowerWhisker, lowerQuantile, median, upperQuantile, upperWhisker)
      }
    }
  }

  /////////////////////////
  // Min & man From Repo //
  /////////////////////////

  override def getMinMax[T](
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[(Option[T], Option[T])] = {
    val fieldType = ftf(field.fieldTypeSpec).asValueOf[T]

    // min and max futures
    val minFuture = dataRepo.min(field.name, criteria, true)
    val maxFuture = dataRepo.max(field.name, criteria, true)

    for {
      minOption <- minFuture
      maxOption <- maxFuture
    } yield
      minOption.zip(maxOption).headOption.map { case (minJsValue, maxJsValue) =>
        val min = fieldType.jsonToValue(minJsValue)
        val max = fieldType.jsonToValue(maxJsValue)
        (min, max)
      }.getOrElse(
        (None, None)
      )
  }

  override def getNumericMinMax(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    field: Field
  ): Future[(Option[Double], Option[Double])] = {

    // aux function to convert the result to double
    def aux[T](toDouble: T => Double) =
      getMinMax[T](dataRepo, criteria, field).map {
        case (min, max) => (min.map(toDouble), max.map(toDouble))
      }

    field.fieldType match {
      case FieldTypeId.Double => aux[Double](identity)
      case FieldTypeId.Integer => aux[Long](_.toDouble)
      case FieldTypeId.Date => aux[ju.Date](_.getTime.toDouble)
      case _ => Future(None, None)
    }
  }

  /////////////////
  // Gram Matrix //
  /////////////////

  override def calcGramMatrix(
    matrix: Traversable[Seq[Double]]
  ): Seq[Seq[Double]] = {
    val squareMatrix = matrix.map(_.map(value => -0.5 * value * value)).toSeq

    // calc row and column sums
    val (rowMeans, columnMeans) = MatrixRowColumnMeanCalc.fun_(squareMatrix)

    // calc total mean
    val totalMean = rowMeans.sum / rowMeans.size

    // produce Gram matrix
    (squareMatrix, rowMeans).zipped.map { case (row, rowMean) =>
      (row, columnMeans).zipped.map { case (value, columnMean) =>
        value - rowMean - columnMean + totalMean
      }
    }
  }

  def calcGramMatrix(
    source: Source[Seq[Double], _]
  ): Future[Seq[Seq[Double]]] = {
    val squareMatrixSource = source.map(_.map(value => -0.5 * value * value))

    for {
      // calc row and column sums
      (rowMeans, columnMeans) <- MatrixRowColumnMeanCalc.runFlow_(squareMatrixSource)

      // Gram matrix
      gramMatrix <- {
        // calc total mean
        val totalMean = rowMeans.sum / rowMeans.size

        val rowMeanSource = Source.fromIterator(() => rowMeans.iterator)

        // produce Gram matrix
        AkkaStreamUtil.zipSources(squareMatrixSource, rowMeanSource).map { case (row, rowMean) =>
          (row, columnMeans).zipped.map { case (value, columnMean) =>
            value - rowMean - columnMean + totalMean
          }
        }.runWith(Sink.seq[Seq[Double]])
      }
    } yield
      gramMatrix
  }

  ////////////////
  // Metric MDS //
  ////////////////

  override def performMetricMDS(
    distanceMatrix: Traversable[Seq[Double]],
    dims: Int,
    scaleByEigenValues: Boolean
  ): Future[(Seq[Seq[Double]], Seq[Double])] = {
    logger.info("Calculating Gram matrix...")
    val gramMatrix = calcGramMatrix(distanceMatrix)

    logger.info("Performing metric MDS...")
    performMetricMDSAux(gramMatrix, dims, scaleByEigenValues)
  }

  override def performMetricMDS(
    distanceMatrixSource: Source[Seq[Double], _],
    dims: Int,
    scaleByEigenValues: Boolean
  ): Future[(Seq[Seq[Double]], Seq[Double])] =
    for {
      gramMatrix <- {
        logger.info("Calculating Gram matrix...")
        calcGramMatrix(distanceMatrixSource)
      }

      result <- {
        logger.info("Performing metric MDS...")
        performMetricMDSAux(gramMatrix, dims, scaleByEigenValues)
      }
    } yield
      result

  private def performMetricMDSAux(
    gramMatrix: Seq[Seq[Double]],
    dims: Int,
    scaleByEigenValues: Boolean
  ) =
    Future {
      val (eigenValues, eigenVectors) = calcEigenValuesAndVectorsSymMatrixBreeze(gramMatrix)
//      val (eigenValues, _, eigenVectors) = calcEigenValuesAndVectorsBreeze(gramMatrix)

      val mdsSolution = eigenVectors.transpose.map(_.take(dims))

      def scaledMdsSolution =
        mdsSolution.transpose.zip(eigenValues).map { case (mdsColumn, eigenValue) =>
          val squareSum = mdsColumn.fold(0d) { case (sum, value) => sum + value * value }
          val factor = Math.sqrt(eigenValue / squareSum)
          mdsColumn.map(_ * factor)
        }.transpose

      (if (scaleByEigenValues) scaledMdsSolution else mdsSolution, eigenValues)
    }

  /////////////////
  // Eigen Stuff //
  /////////////////

  override def calcEigenValuesAndVectors(
    matrix: Seq[Seq[Double]]
  ): (Seq[Double], Seq[Seq[Double]]) = {
    val realMatrix = new Array2DRowRealMatrix(matrix.map(_.toArray).toArray)
    val eigenDecomposition = new EigenDecomposition(realMatrix)

    // eigen values
    val eigenValues = eigenDecomposition.getRealEigenvalues.toSeq

    // eigen vectors
    val eigenVectorMatrix = eigenDecomposition.getVT
    val eigenVectors = for (i <- 0 to eigenVectorMatrix.getRowDimension - 1) yield eigenVectorMatrix.getRow(i).toSeq

    (eigenValues, eigenVectors)
  }

  override def calcEigenValuesAndVectorsSymMatrixBreeze(
    matrix: Seq[Seq[Double]]
  ): (Seq[Double], Seq[Seq[Double]]) = {
    val EigSym(eigenValues, eigenVectors) = eigSym(DenseMatrix(matrix: _*))

    (eigenValues.toScalaVector().reverse, eigenVectors.data.toSeq.grouped(eigenVectors.rows).toSeq.reverse)
  }

  override def calcEigenValuesAndVectorsBreeze(
    matrix: Seq[Seq[Double]]
  ): (Seq[Double], Seq[Double], Seq[Seq[Double]]) = {
    val result = eig(DenseMatrix(matrix: _*))

    val eigenValues = result.eigenvalues.toScalaVector()
    val eigenValuesComplex = result.eigenvaluesComplex.toScalaVector()
    val eigenVectors = result.eigenvectors.data.toSeq.grouped(result.eigenvectors.rows).toSeq

    val sortedResult = (eigenValues, eigenValuesComplex, eigenVectors).zipped.toSeq.sortBy(-_._1)

    (
      sortedResult.map(_._1),
      sortedResult.map(_._2),
      sortedResult.map(_._3)
    )
  }

  ///////////
  // t-SNE //
  ///////////

  private val parallelTSNE = true
  private val silentTSNSE = true

  override def performTSNE(
    data: Array[Array[Double]],
    setting: TSNESetting
  ) = {
    logger.info(s"Running t-SNE for ${data.length} items with ${data(0).length} features and ${setting.maxIterations} iterations.")
    val tsne = if (parallelTSNE) new ParallelBHTsne else new BHTSne
    val config = new TSneConfig(
      data, setting.dims, setting.pcaDims.getOrElse(0), setting.perplexity, setting.maxIterations, setting.pcaDims.isDefined, setting.theta, silentTSNSE, true
    )
    tsne.tsne(config)
  }

  /////////////////////
  // Standardization //
  /////////////////////

  def standardize(
    inputs: Traversable[Seq[Option[Double]]],
    useSampleStd: Boolean
  ): Traversable[Seq[Option[Double]]] = {
    val basicStats = MultiBasicStatsCalc.fun()(inputs)
    StandardizationCalc.fun(meansAndStds(basicStats, useSampleStd))(inputs)
  }

  def standardizeSimple(
    source: Source[Seq[Option[Double]], _],
    useSampleStd: Boolean
  ): Future[Traversable[Seq[Option[Double]]]] =
    for {
      basicStats <- MultiBasicStatsCalc.runFlow_(source)

      result <- StandardizationCalc.runFlow(meansAndStds(basicStats, useSampleStd), ())(source)
    } yield
      result

  def standardize[T](
    source: Source[(T, Seq[Option[Double]]), _],
    useSampleStd: Boolean
  ): Future[Traversable[(T, Seq[Option[Double]])]] =
    for {
      basicStats <- MultiBasicStatsCalc.runFlow_(source.map(_._2))

      results <- {
        val resultFlow = StandardizationCalc.nonSeqFlow(meansAndStds(basicStats, useSampleStd))
        val identityFlow = Flow[T]

        val mergedFlow = AkkaStreamUtil.applyTupleFlows(identityFlow, resultFlow)

        source.via(mergedFlow).runWith(Sink.seq)
      }
    } yield
      results

//  def standardizeX(
//    source: Source[JsObject, _],
//    fields: Seq[Field],
//    useSampleStd: Boolean
//  ): Future[Traversable[Seq[Option[Double]]]] =
//    for {
//      basicStats <- multiBasicStatsExec.execJsonStreamed_(fields)(source)
//
//      standardizationOptions = meansAndStds(basicStats, useSampleStd)
//
//      result <- standardizationExec.execJsonStreamed(standardizationOptions, (), fields)(source)
//    } yield
//      result

  private def meansAndStds(
    basicStats: Seq[Option[BasicStatsResult]],
    useSampleStd: Boolean
  ) =
    basicStats.map(
      _ match {
        case Some(stats) => (stats.mean, if (useSampleStd) stats.sampleStandardDeviation else stats.standardDeviation)
        case None => (0d, 0d)
      }
    )

  ////////////////////////
  // Independence Tests //
  ////////////////////////

  override def testIndependenceSorted(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean
  ): Future[Seq[(Field, Option[IndependenceTestResult])]] =
    testIndependence(dataRepo, criteria, inputFields, targetField, keepUndefined).map { results =>
      sortIndependenceTestResults(results, inputFields)
    }

  override def testIndependenceSortedJson(
    items: Traversable[JsObject],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean
  ): Seq[(Field, Option[IndependenceTestResult])] = {
    val results = testIndependence(items, inputFields, targetField, keepUndefined)
    sortIndependenceTestResults(results, inputFields)
  }

  private val chiSquareTestExec = multiChiSquareTestExec[Option[Any], Any]
  private val anovaTestExec = multiOneWayAnovaTestExec[Option[Any]]
  private val nullExcludedChiSquareTestExec = nullExcludedMultiChiSquareTestExec[Any, Any]
  private val nullExcludedAnovaTestExec = nullExcludedMultiOneWayAnovaTestExec[Any]

  override def testIndependence(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean
  ): Future[Seq[Option[IndependenceTestResult]]] = {
    val numericInputFields = inputFields.filter(_.isNumeric)
    val nonNumericInputFields = inputFields.filter(!_.isNumeric)

    val chiSquareResultsFuture =
      if (keepUndefined)
        chiSquareTestExec.execJsonRepoStreamed_(withProjection = true, Seq(targetField) ++ nonNumericInputFields)(dataRepo, criteria)
      else
        nullExcludedChiSquareTestExec.execJsonRepoStreamed_(withProjection = true, Seq(targetField) ++ nonNumericInputFields)(dataRepo, criteria)

    val anovaResultsFuture =
      if (keepUndefined)
        anovaTestExec.execJsonRepoStreamed_(withProjection = true, Seq(targetField) ++ numericInputFields)(dataRepo, criteria)
      else
        nullExcludedAnovaTestExec.execJsonRepoStreamed_(withProjection = true, Seq(targetField) ++ numericInputFields)(dataRepo, criteria)

    for {
      chiSquareResults <- chiSquareResultsFuture
      anovaResults <- anovaResultsFuture
    } yield {
      val fieldChiSquareResultMap = nonNumericInputFields.zip(chiSquareResults).toMap
      val fieldAnovaResultMap = numericInputFields.zip(anovaResults).toMap

      inputFields.map( field =>
        if (field.isNumeric)
          fieldAnovaResultMap.get(field).get
        else
          fieldChiSquareResultMap.get(field).get
      )
    }
  }

  private def testIndependence(
    items: Traversable[JsObject],
    inputFields: Seq[Field],
    targetField: Field,
    keepUndefined: Boolean
  ): Seq[Option[IndependenceTestResult]] = {
    val numericInputFields = inputFields.filter(_.isNumeric)
    val nonNumericInputFields = inputFields.filter(!_.isNumeric)

    val chiSquareResults =
      if (keepUndefined)
        chiSquareTestExec.execJson_(Seq(targetField) ++ nonNumericInputFields)(items)
      else
        nullExcludedChiSquareTestExec.execJson_(Seq(targetField) ++ nonNumericInputFields)(items)

    val anovaResults =
      if (keepUndefined)
        anovaTestExec.execJson_(Seq(targetField) ++ numericInputFields)(items)
      else
        nullExcludedAnovaTestExec.execJson_(Seq(targetField) ++ numericInputFields)(items)

    val fieldChiSquareResultMap = nonNumericInputFields.zip(chiSquareResults).toMap
    val fieldAnovaResultMap = numericInputFields.zip(anovaResults).toMap

    inputFields.map( field =>
      if (field.isNumeric)
        fieldAnovaResultMap.get(field).get
      else
        fieldChiSquareResultMap.get(field).get
    )
  }

  override def selectFeaturesAsAnovaChiSquare(
    dataRepo: JsonReadonlyRepo,
    criteria: Seq[Criterion[Any]],
    inputFields: Seq[Field],
    targetField: Field,
    featuresToSelectNum: Int,
    keepUndefined: Boolean
  ): Future[Seq[Field]] =
    testIndependenceSorted(dataRepo, criteria, inputFields, targetField, keepUndefined).map { results =>
      results.map(_._1).take(featuresToSelectNum)
    }

  private val multiBasicStatsExec = multiBasicStatsSeqExec

  override def testAnovaForMultiCriteriaSorted(
    dataRepo: JsonReadonlyRepo,
    multiCriteria: Seq[Seq[Criterion[Any]]],
    inputFields: Seq[Field]
  ): Future[Seq[(Field, Option[OneWayAnovaResult])]] = {
    val numericInputFields = inputFields.filter(_.isNumeric)

    // aux function to pull the required data (for given criteria) and cal basic stats for each field
    def calcStatsAux(
      criteria: Seq[Criterion[Any]]
    ): Future[Seq[Option[BasicStatsResult]]] =
      dataRepo.find(criteria = criteria, projection = inputFields.map(_.name)).map(
        multiBasicStatsExec.execJson_(numericInputFields)
      )

    for {
      multiStats <- Future.sequence(multiCriteria.map(calcStatsAux))
    } yield {
      // aux function to calculate ANOVA at a given position/field
      def calcAt(index: Int) = calcAnovaFromStats(multiStats.flatMap(_(index)))

      val elementsCount = if (multiStats.nonEmpty) multiStats.head.size else 0

      val results = (0 until elementsCount).par.map(calcAt).toList

      sortIndependenceTestResults(results, inputFields)
    }
  }

  private val multiCountDistinctExec = multiCountDistinctSeqExec[Option[Any]]

  override def testChiSquareForMultiCriteriaSorted(
    dataRepo: JsonReadonlyRepo,
    multiCriteria: Seq[Seq[Criterion[Any]]],
    inputFields: Seq[Field]
  ): Future[Seq[(Field, Option[ChiSquareResult])]] = {

    // aux function to pull the required data (for given criteria) and cal basic stats for each field
    def calcCountsAux(
      criteria: Seq[Criterion[Any]]
    ): Future[Seq[Traversable[(Option[Any], Int)]]] =
      dataRepo.find(criteria = criteria, projection = inputFields.map(_.name)).map(
        multiCountDistinctExec.execJson_(inputFields)
      )

    for {
      multiCounts <- Future.sequence(multiCriteria.map(calcCountsAux))
    } yield {
      // aux function to calculate Chi-Square at a given position/field
      def calcAt(index: Int) = {
        val groupCounts = multiCounts.map(_(index))

        val counts = groupCounts.zipWithIndex.flatMap { case (valueCounts, groupIndex) =>
          valueCounts.map { case (value, count) =>
            (groupIndex, value) -> count
          }
        }.toMap

        calcChiSquareSafe(counts)
      }

      val elementsCount = if (multiCounts.nonEmpty) multiCounts.head.size else 0

      val results = (0 until elementsCount).par.map(calcAt).toList

      sortIndependenceTestResults(results, inputFields)
    }
  }

  private def sortIndependenceTestResults[T <: IndependenceTestResult](
    results: Seq[Option[T]],
    inputFields: Seq[Field]
  ): Seq[(Field, Option[T])] = {
    // Sort and combine the results
    def pValueAndStat(result: Option[T]): (Double, Double) =
      result.map {
        _ match {
          case ChiSquareResult(pValue, statistics, _) => (pValue, statistics)
          case OneWayAnovaResult(pValue, fValue, _, _) => (pValue, fValue)
        }
      }.getOrElse((Double.PositiveInfinity, 0d))

    inputFields.zip(results).sortWith { case ((fieldName1, result1), (fieldName2, result2)) =>
      val (pValue1, stat1) = pValueAndStat(result1)
      val (pValue2, stat2) = pValueAndStat(result2)

      (pValue1 < pValue2) || (pValue1 == pValue2 && stat1 > stat2)
    }
  }
}

case class TSNESetting(
  dims: Int = 2,
  perplexity: Double = 20,
  theta: Double = 0.5,
  maxIterations: Int = 1000,
  pcaDims: Option[Int] = None
)