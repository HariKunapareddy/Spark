package com.hari.project.Spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.AnalysisException;

public class SparkSql {

	public static void main(String[] args) throws AnalysisException {
		SparkSession session=SparkSession.builder().appName("SQL").master("local[*]").getOrCreate();
		Dataset<Row> people= session.read().option("header", true).csv("people.csv");
		Dataset<Row> department= session.read().option("header", true).csv("department.csv");
		people.show();
		department.show();
		//people.filter(people.col("age").gt("30")).
		//join(department,people.col("deptId").equalTo(department.col("id"))).groupBy(department.col("name")).count().show();
		
		Dataset<Row> emp= session.read().option("header", true).csv("emp.csv");
		emp.groupBy("loc").agg(max("age"),max("salary")).show();
		emp.createGlobalTempView("emp");
		
				//emp.groupBy("loc").avg("salary").show();;
				
				session.sql("select * from emp").show();
	/*	
		+-------+--------+-----------+
		|    loc|max(age)|max(salary)|
		+-------+--------+-----------+
		|chennai|      32|     410000|
		|    ban|      39|     530000|
		|    Hyd|      40|     709000|
		|    mys|      39|     600600|
		+-------+--------+-----------+
		*/
	}
}
