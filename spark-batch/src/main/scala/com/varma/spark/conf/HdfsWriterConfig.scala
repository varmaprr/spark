package com.varma.spark.conf

import com.typesafe.config.Config
import org.spark_project.guava.base.Preconditions

/**
  * Created by varma on 6/24/2018.
  */
class HdfsWriterConfig extends IConfig {

  var path: String = null;
  var codec: String = null;
  var mode: String = null;

  override def parser(conf: Config): IConfig = {
    path = conf.getString("path");
    Preconditions.checkNotNull(path);
    codec = conf.getString("codec");
    Preconditions.checkNotNull(codec);
    mode = conf.getString("mode");
    Preconditions.checkNotNull(mode);
    return this;
  }

}
