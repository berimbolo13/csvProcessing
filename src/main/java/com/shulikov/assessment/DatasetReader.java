package com.shulikov.assessment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class DatasetReader {

    public static Dataset<Row> read(String csvPath) {

        StructType schema = new StructType()
                .add("timestamp", "string")
                .add("unit", "string")
                .add("sensoridentifier", "string")
                .add("samplingRate", "int")
                .add("status", "int")
                .add("value_nominal", "double");

        return SparkSession
                .builder()
                .appName("Assessment")
                .config("spark.master", "local")
                .config("spark.driver.memory", "501859200")
                .getOrCreate()
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .csv(csvPath);
    }
}
