package com.varma.spark.scala

import java.util

import collection.JavaConversions._
import com.google.common.io.Resources
import com.varma.spark.conf.{GrokParserConfig, IConfig}
import com.varma.spark.utils.{NewRecord, Record}
import io.krakens.grok.api.{Grok, GrokCompiler, Match}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by varma on 6/24/2018.
  */
class GrokParser(conf: IConfig, df: DataFrame, session: SparkSession) extends Serializable {

  val LOG = LoggerFactory.getLogger(getClass.getName);

  val grokParser: GrokParserConfig = conf.asInstanceOf[GrokParserConfig];

  def apply(): DataFrame = {

    import df.sparkSession.implicits._;

    val schema: StructType = new StructType().add("MONTH", DataTypes.StringType, false).
      add("COMMONAPACHELOG", DataTypes.StringType, false).
      add("auth", DataTypes.StringType, false).
      add("HOUR", DataTypes.StringType, false);

    val x = df.rdd.map(record => applyToRow(record));

    return session.createDataFrame(x, schema);
  }

  def applyToRow(record: Row): Row = {

    val compiler = GrokCompiler.newInstance();
    compiler.registerDefaultPatterns();
    compiler.register(Resources.getResource(grokParser.patternFile).openStream);
    compiler.register("foo", "%{COMMONAPACHELOG}")

    val grok: Grok = compiler.compile("%{foo}")
    val `match`: Match = grok.`match`(record.toString())
    val map: util.Map[String, AnyRef] = `match`.capture

    RowFactory.create(map.getOrDefault("MONTH", "").toString, map.getOrDefault("auth", "").toString,
      map.getOrDefault("HOUR", "").toString, map.getOrDefault("ident", "").toString);
  }

}
