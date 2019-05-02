package org.ada.server.services

import javax.inject.{Inject, Singleton}

import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{monotonically_increasing_id, struct, udf}
import org.apache.spark.sql.types.StructType
import play.api.Configuration
import play.api.libs.json._

@Singleton
class SparkApp @Inject() (configuration: Configuration) {

  private val reservedKeys = Set("spark.master.url", "spark.driver.jars")

  private val settings = configuration.keys.filter(key =>
    key.startsWith("spark.") && !reservedKeys.contains(key)
  ).flatMap { key =>
    println(key)
    configuration.getString(key).map((key, _))
  }

  private val conf = new SparkConf(false)
    .setMaster(configuration.getString("spark.master.url").getOrElse("local[*]"))
    .setAppName("Ada")
    .set("spark.logConf", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.worker.cleanup.enabled", "true")
    .set("spark.worker.cleanup.interval", "900")
    .setJars(configuration.getStringSeq("spark.driver.jars").getOrElse(Nil))
    .setAll(settings)
    .registerKryoClasses(Array(
      classOf[scala.collection.mutable.ArraySeq[_]]
    ))

  val session = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val sc = session.sparkContext

  val sqlContext = session.sqlContext
}