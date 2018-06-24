package main.scala.com.varma.spark.scala

import com.typesafe.config.ConfigFactory
import com.varma.spark.conf.{GrokParserConfig, HdfsReaderConfig, HdfsWriterConfig, SparkConfig}
import com.varma.spark.scala.GrokParser
import com.varma.spark.utils.{ConfigType, Formatter, Properties}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.spark_project.guava.io.Resources

import scala.collection.JavaConversions._

/**
  * Created by varma on 6/23/2018.
  */
object SparkJobRunner {

  val LOG = LoggerFactory.getLogger(getClass.getName);

  def main(args: Array[String]): Unit = {

    try {

      LOG.info("loading configurations!!! ");

      val config = ConfigFactory.load();

      val sparkConf: SparkConfig = ConfigType.spark.getObj(config.getConfig("application.spark")).asInstanceOf[SparkConfig];
      val readerConfig: HdfsReaderConfig = ConfigType.hdfsReader.getObj(config.getConfig("application.hdfsReader")).asInstanceOf[HdfsReaderConfig];
      val writerConfig: HdfsWriterConfig = ConfigType.hdfsWriter.getObj(config.getConfig("application.hdfsWriter")).asInstanceOf[HdfsWriterConfig];
      val grokParserConfig: GrokParserConfig = ConfigType.grokparser.getObj(config.getConfig("application.grokParser")).asInstanceOf[GrokParserConfig];

      LOG.info("configuration loaded!! ");

      LOG.info("Spark process is running... hold on !!");

      var spark = SparkSession.builder().appName(sparkConf.applicationName).master(sparkConf.master);

      for (e: Properties <- sparkConf.properties) spark = spark.config(e.key, e.value);

      val sparkSession = spark.getOrCreate();

      var df = sparkSession.read.format("text").load(Resources.getResource(readerConfig.path).getPath).toDF("message");

      df = new GrokParser(grokParserConfig, df).apply();

      df.write.mode(writerConfig.mode).format(Formatter.valueOf(writerConfig.codec).toString).save(writerConfig.path);

      LOG.info("Spark process completed, enjoy :) !!");
    }
    catch {
      case e: Exception => LOG.error(e.getMessage);
    }
  }

}