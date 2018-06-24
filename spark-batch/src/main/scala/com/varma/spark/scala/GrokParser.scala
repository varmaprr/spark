package com.varma.spark.scala

import java.util

import collection.JavaConversions._
import com.google.common.io.Resources
import com.varma.spark.conf.{GrokParserConfig, IConfig}
import com.varma.spark.utils.{NewRecord, Record}
import io.krakens.grok.api.{Grok, GrokCompiler, Match}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import org.slf4j.LoggerFactory

/**
  * Created by varma on 6/24/2018.
  */
class GrokParser(conf: IConfig, df: DataFrame) extends Serializable {

  val LOG = LoggerFactory.getLogger(getClass.getName);

  val grokParser: GrokParserConfig = conf.asInstanceOf[GrokParserConfig];

  def apply(): DataFrame = {

    import df.sparkSession.implicits._;

    val x = df.as[Record].map(record => applyToRow(record.message, record));

    return x.toDF();
  }

  def applyToRow(str: String, record: Record): NewRecord = {

    val compiler = GrokCompiler.newInstance();
    compiler.registerDefaultPatterns();
    compiler.register(Resources.getResource(grokParser.patternFile).openStream);
    compiler.register("foo", "%{COMMONAPACHELOG}")

    val grok: Grok = compiler.compile("%{foo}")
    val `match`: Match = grok.`match`(str)
    val map: util.Map[String, AnyRef] = `match`.capture
    //val tuple = map.map(e => e._2);

    //RowFactory.create(tuple);

    return NewRecord(MONTH  = map.getOrDefault("MONTH","").toString,auth  = map.getOrDefault("auth","").toString,
      HOUR  = map.getOrDefault("HOUR","").toString, ident  = map.getOrDefault("ident","").toString, foo  = map.getOrDefault("foo","").toString, verb  = map.getOrDefault("verb","").toString,
      TIME  = map.getOrDefault("TIME","").toString,INT  = map.getOrDefault("INT","").toString,
      YEAR  = map.getOrDefault("YEAR","").toString,response  = map.getOrDefault("response","").toString,bytes  = map.getOrDefault("bytes","").toString,
      clientip  = map.getOrDefault("clientip", "").toString,MINUTE  = map.getOrDefault("MINUTE","").toString,SECOND  = map.getOrDefault("SECOND", "").toString) ;


  }

}
