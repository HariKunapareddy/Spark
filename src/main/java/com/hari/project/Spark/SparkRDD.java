package com.hari.project.Spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class SparkRDD {
	public static void main(String[] args) {
		SparkSession session=SparkSession.builder().appName("SparkRdd").getOrCreate();
		SparkContext context=session.sparkContext();
		JavaSparkContext javaContext=new JavaSparkContext(context);
		JavaRDD<String> rdd= javaContext.textFile("stockRecods.csv");
		
		JavaRDD<Object> mapRdd = rdd.map(row->{
			return row.split(" ");
		});
		
	
		JavaRDD<Object>  flatMapRdd =rdd.flatMap(row->{
			
			return (Iterator<Object>) Arrays.asList(row.split(" "));
			});
	}

}
