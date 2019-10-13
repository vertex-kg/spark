package com.softlinqs.lab.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

import static java.util.Arrays.asList;

public class DataframeUtil {

    public Dataset<Row> createDataframe(SQLContext sqlContext) {

        List<Item> items = asList(
                new Item("20191012", "keyA1", "valueA"),
                new Item("20191012", "keyB1", "valueB"),
                new Item("20191014", "keyD1", "valueD"),
                new Item("20191015", "keyE1", "valueE"),
                new Item("20191012", "keyC1", "valueC"),
                new Item("20191013", "keyA", "valueA")
        );

        Dataset<Row> df = sqlContext.createDataFrame(items, Item.class).withColumnRenamed("runDate", "run_date");
        df.show();

        return df;
    }
}
