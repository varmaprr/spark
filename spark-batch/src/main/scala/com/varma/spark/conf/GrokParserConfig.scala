package com.varma.spark.conf
import com.google.common.base.Preconditions
import com.typesafe.config.Config

/**
  * Created by varma on 6/24/2018.
  */
class GrokParserConfig extends IConfig {

  var patternFile:String = null;

  override def parser(conf: Config): IConfig = {
    patternFile = conf.getString("pattern-file");
    Preconditions.checkNotNull(patternFile);
    return this;
  }

}
