package com.varma.spark.conf

import com.typesafe.config.Config
import java.util.List
import com.varma.spark.utils.Properties
import org.spark_project.guava.base.Preconditions

/**
  * Created by varma on 6/24/2018.
  */
class SparkConfig extends IConfig {

  var applicationName: String = null;
  var master: String = null;
  var properties: List[Properties] = null;

  override def parser(conf: Config): IConfig = {
    applicationName = conf.getString("applicationName");
    Preconditions.checkNotNull(applicationName);
    master = conf.getString("master");
    Preconditions.checkNotNull(master);
    properties = getProperties(conf);
    Preconditions.checkNotNull(properties);
    return this;
  }

  def getProperties(config: Config): List[Properties] = {

    val properties = config.getConfigList("properties");
    Preconditions.checkNotNull(properties);

    val providerList = new java.util.ArrayList[Properties]

    val providers = (0 until properties.size())

    providers foreach {
      count =>
        val iterator = properties.get(count).entrySet().iterator()

        while (iterator.hasNext()) {
          val entry = iterator.next()
          val p = new Properties(entry.getKey(), entry.getValue().render())
          providerList.add(p);
        }
    }

    return providerList;
  }


}
