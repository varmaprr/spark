package com.varma.spark.utils;

/**
 * Created by varma on 6/24/2018.
 */
public enum Formatter {
    CSV("com.databricks.spark.csv"),
    AVRO("com.databricks.spark.avro"),
    PARQUET("parquet"),
    TEXT("text");

    private final String name;

    Formatter(String s) {
        name = s;
    }

    public String toString() {
        return this.name;
    }
}
