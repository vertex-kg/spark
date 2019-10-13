package com.softlinqs.lab.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public interface SparkSessionTestWrapper {

    SparkSession spark = SparkSession
            .builder()
            .enableHiveSupport()
            .appName("LocalSparkApplication")
            .master("local[*]")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
//            .config("spark.sql.warehouse.dir", "hive")
            .getOrCreate();

    SQLContext sqlContext = spark.sqlContext();

}
