package com.apachespark.sparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Application {

	public static void main(String[] args) {
		// Create Session
		SparkSession spark = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();
		// Reading and import Data
		Dataset<Row> d0Df = spark.read().format("CSV").option("header", true)
				.load("src/main/resources/account_and_fields.txt");
			
		Dataset<Row> d1Df = spark.read().format("CSV").option("header", true)
				.load("src/main/resources/account_and_fields2.txt");
		//Merging 2 files with same schema
		Dataset<Row> unionDf = d1Df.unionByName(d0Df);
		//Finding the unchanged rows using intersect function
		Dataset<Row> commonDf = d0Df.intersect(d1Df);
		
		Dataset<Row> addedDf = unionDf.except(commonDf).except(d0Df).drop("field2").drop("field1").drop("field3");
//UPDATE+ADDED 
		Dataset<Row> deletedDf = unionDf.except(commonDf).except(d1Df).drop("field2").drop("field1")
				.drop("field3");
//UPDATE+DELETED
		Dataset<Row> updatedDf = addedDf.intersect(deletedDf);
		addedDf = addedDf.except(updatedDf).withColumn("STATUS", lit("Added"));
		deletedDf = deletedDf.except(updatedDf).withColumn("STATUS", lit("Deleted"));

		updatedDf = updatedDf.withColumn("STATUS", lit("Updated"));
		commonDf = commonDf.withColumn("STATUS", lit("Unchanged")).drop("field2").drop("field1").drop("field3");
		Dataset<Row> deltaDf = commonDf.unionByName(deletedDf).unionByName(updatedDf).unionByName(addedDf)
				.orderBy(commonDf.col("account_number").asc());
		deltaDf.show();
	}

}
