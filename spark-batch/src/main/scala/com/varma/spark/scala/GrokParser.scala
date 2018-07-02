package com.varma.spark.scala

import java.util

import collection.JavaConversions._
import com.google.common.io.Resources
import com.varma.spark.conf.{GrokParserConfig, IConfig}
import com.varma.spark.utils.StringUtils
import io.krakens.grok.api.{Grok, GrokCompiler, Match}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by varma on 6/24/2018.
  */
class GrokParser(conf: IConfig, df: DataFrame, session: SparkSession) extends Serializable {

  val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val grokParser: GrokParserConfig = conf.asInstanceOf[GrokParserConfig]

  def apply(): DataFrame = {

    val col_vector = for (i <- 0 until 19) yield StructField("col_" + i, DataTypes.StringType, false)

    val schema: StructType = StructType(col_vector.toList)

    val x = df.rdd.mapPartitions(records => records.toList.map(record => applyToRow(record)).iterator)

    session.createDataFrame(x, schema)

  }

  def applyToRow(record: Row): Row = {

    val compiler = GrokCompiler.newInstance()
    compiler.registerDefaultPatterns()
    compiler.register(Resources.getResource(grokParser.patternFile).openStream)
    compiler.register("foo", "%{COMMONAPACHELOG}")

    LOG.debug("Source Message : " + record.toString())

    val grok: Grok = compiler.compile("%{foo}")
    val `match`: Match = grok.`match`(record.toString())
    val map: util.Map[String, AnyRef] = `match`.capture

    val resultsVector = for ((_, v) <- map) yield StringUtils.getStringOrDefault(v, "")

    LOG.debug("Grok parse result : " + resultsVector.toSeq)

    Row.fromSeq(resultsVector.toSeq)
  }

}
