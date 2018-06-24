package com.varma.spark.scala

import java.util
import java.util.Map

import collection.JavaConversions._
import com.google.common.io.Resources
import com.varma.spark.conf.{GrokParserConfig, IConfig}
import com.varma.spark.utils.Record
import io.krakens.grok.api.{Grok, GrokCompiler, Match}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Created by varma on 6/24/2018.
  */
class GrokParser(conf: IConfig, df: DataFrame) extends Serializable {

  val LOG = LoggerFactory.getLogger(getClass.getName);

  val grokParser: GrokParserConfig = conf.asInstanceOf[GrokParserConfig];

  def apply(): DataFrame = {

    import df.sparkSession.implicits._;

    return df.as[Record].map(record => applyToRow(record.message, record)).toDF();
  }

  def applyToRow(str: String, record: Record): Record = {

    val compiler = GrokCompiler.newInstance();
    compiler.register(Resources.getResource(grokParser.patternFile).openStream);
    compiler.register("bar", ".*")
    compiler.register("foo", ".*")

    val grok: Grok = compiler.compile("%{foo}")
    val `match`: Match = grok.`match`(str)
    val map: util.Map[String, AnyRef] = `match`.capture
    for ((k, v) <- map) LOG.info("key: " + k + " value: " + v);

    return record;
  }

}
