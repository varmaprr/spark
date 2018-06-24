package com.varma.spark.conf

import com.typesafe.config.Config
import org.spark_project.guava.base.Preconditions

/**
  * Created by varma on 6/24/2018.
  */
class HdfsReaderConfig extends IConfig {

  var path: String = null;

  override def parser(conf: Config): IConfig = {
    path = conf.getString("path");
    Preconditions.checkNotNull(path);
    return this;
  }

}
