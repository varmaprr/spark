package com.varma.spark.utils;

import com.typesafe.config.Config;
import com.varma.spark.conf.*;

/**
 * Created by varma on 6/24/2018.
 */
public enum ConfigType {

    spark(new SparkConfig()),
    hdfsReader(new HdfsReaderConfig()),
    hdfsWriter(new HdfsWriterConfig()),
    grokparser(new GrokParserConfig());

    ConfigType(IConfig conf) {
        this.obj = conf;
    }

    private IConfig obj;

    public IConfig getObj(Config conf) {
        return obj.parser(conf);
    }

}
