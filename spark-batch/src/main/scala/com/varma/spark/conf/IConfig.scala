package com.varma.spark.conf

import com.typesafe.config.Config;

/**
  * Created by varma on 6/24/2018.
  */
abstract class IConfig extends Serializable{

  def parser(conf:Config) : IConfig

}
