package org.ada.server.services.ml

import org.ada.server.models.ml.unsupervised.{UnsupervisedLearning, BisectingKMeans => BisectingKMeansDef, GaussianMixture => GaussianMixtureDef, KMeans => KMeansDef, LDA => LDADef}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans, LDA}
import org.incal.spark_ml.{ParamGrid, ParamSourceBinder, SparkMLEstimatorFactoryHelper}

object SparkUnsupervisedEstimatorFactory extends SparkMLEstimatorFactoryHelper {

  def apply[M <: Model[M]](
    model: UnsupervisedLearning
  ): Estimator[M] =
    model match {
      case x: KMeansDef => applyAux(x).asInstanceOf[Estimator[M]]
      case x: LDADef => applyAux(x).asInstanceOf[Estimator[M]]
      case x: BisectingKMeansDef => applyAux(x).asInstanceOf[Estimator[M]]
      case x: GaussianMixtureDef => applyAux(x).asInstanceOf[Estimator[M]]
    }

  private def applyAux(
    model: KMeansDef)
  : KMeans = {
    val (estimator, _) = ParamSourceBinder(model, new KMeans())
      .bind(_.initMode.map(_.toString), "initMode")
      .bind(_.initSteps, "initSteps")
      .bind({o => Some(o.k)}, "k")
      .bind(_.maxIteration, "maxIter")
      .bind(_.tolerance, "tol")
      .bind(_.seed, "seed")
      .build

    estimator
  }

  private def applyAux(
    model: LDADef
  ): LDA = {
    val (estimator, _) = ParamSourceBinder(model, new LDA())
      .bind(_.checkpointInterval, "checkpointInterval")
      .bind(_.keepLastCheckpoint, "keepLastCheckpoint")
      .bind[Array[Double]](_.docConcentration.map(_.toArray), "docConcentration")
      .bind(_.optimizeDocConcentration, "optimizeDocConcentration")
      .bind(_.topicConcentration, "topicConcentration")
      .bind({o => Some(o.k)}, "k")
      .bind(_.learningDecay, "learningDecay")
      .bind(_.learningOffset, "learningOffset")
      .bind(_.maxIteration, "maxIter")
      .bind(_.optimizer.map(_.toString), "optimizer")
      .bind(_.subsamplingRate, "subsamplingRate")
      .bind(_.seed, "seed")
      .build

    estimator
  }

  private def applyAux(
    model: BisectingKMeansDef
  ): BisectingKMeans = {
    val (estimator, _) = ParamSourceBinder(model, new BisectingKMeans())
      .bind({o => Some(o.k)}, "k")
      .bind(_.maxIteration, "maxIter")
      .bind(_.seed, "seed")
      .bind(_.minDivisibleClusterSize, "minDivisibleClusterSize")
      .build

    estimator
  }

  private def applyAux(
    model: GaussianMixtureDef
  ): GaussianMixture = {
    val (estimator, _) = ParamSourceBinder(model, new GaussianMixture())
      .bind({ o => Some(o.k) }, "k")
      .bind(_.maxIteration, "maxIter")
      .bind(_.tolerance, "tol")
      .bind(_.seed, "seed")
      .build

    estimator
  }
}