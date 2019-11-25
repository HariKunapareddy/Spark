package com.hari.project.Spark;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkPrac {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			SparkSession session=SparkSession.builder().appName("customapp").getOrCreate();
			SparkContext sc=session.sparkContext();
			JavaSparkContext context=new JavaSparkContext(sc);
			JavaRDD<String> textRDD=context.textFile("people.txt");
			textRDD.flatMap(record->Arrays.asList(record.split(" ")).iterator()).collect();
		
	}

}
