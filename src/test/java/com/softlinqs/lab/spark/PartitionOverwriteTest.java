package com.softlinqs.lab.spark;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Column;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

import static java.lang.String.format;

public class PartitionOverwriteTest implements SparkSessionTestWrapper {

    @BeforeClass
    public static void createDatabase() throws Exception {

        String userDir = System.getProperty("user.dir");

        sqlContext.sql("drop table if exists keys");
        sqlContext.sql(
                "create table if not exists keys (key String, value String, run_date String) " +
                        "using PARQUET " +
                        "partitioned by (run_date) " +
                        format("location '%s/src/test/resources/out/keys.parquet'", userDir));
    }


    @Test
    public void testPartitionOverwrite() throws Exception {

        Dataset<Row> df1 = readCsv("data/sample1.csv");
        Dataset<Row> df2 = readCsv("data/sample2.csv");

        System.out.println("Showing sample1 content");
        df1.show();

        System.out.println("Showing sample2 content");
        df2.show();

        Dataset<Row> df1Aligned = alignColumnsToTable(spark, df1, "keys");
        System.out.println("Showing sample1 aligned dataset");
        df1Aligned.show();

        Dataset<Row> df2Aligned = alignColumnsToTable(spark, df2, "keys");
        System.out.println("Showing sample2 aligned dataset");
        df2Aligned.show();

        Dataset<Row> dataset = sqlContext.sql("select * from keys");
        System.out.println("Showing content of hive table before insert");
        dataset.show();

        insertIntoTable(df1Aligned, "keys");

        dataset = sqlContext.sql("select * from keys");
        System.out.println("Showing content of hive table after sample1 insert");
        dataset.show();

        insertIntoTable(df2Aligned, "keys");

        dataset = sqlContext.sql("select * from keys");
        System.out.println("Showing content of hive table after sample2 insert");
        dataset.show();
    }

    private Dataset<Row> readCsv(String path) {
        return spark.read()
                .format("csv")
                .option("sep", "|")
                .option("header", "true")
                .load(format("src/test/resources/%s", path));
    }

    private void insertIntoTable(Dataset<Row> dataset, String tableName) {
        dataset
                .write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .insertInto(tableName);
    }

    private <T> Seq<T> toSeq(List<T> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    private Dataset<Row> alignColumnsToTable(SparkSession spark, Dataset<Row> dataset, String tableName) throws AnalysisException{
        List<String> columns = spark.catalog().listColumns(tableName)
                .map((MapFunction<Column, String>) Column::name, Encoders.STRING())
                .collectAsList();

        org.apache.spark.sql.Column[] columnList = columns.stream().map(functions::col).toArray(org.apache.spark.sql.Column[]::new);

        return dataset.select(columnList);
    }
}
