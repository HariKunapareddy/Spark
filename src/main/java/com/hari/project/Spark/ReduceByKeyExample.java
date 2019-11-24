package com.hari.project.Spark;


import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class ReduceByKeyExample {
    public static void main(String[] args) throws Exception {
    	
    	SparkSession sesion=SparkSession.builder().appName("reduceByKey").master("local[*]").getOrCreate();
    	SparkContext context=sesion.sparkContext();
        JavaSparkContext sc = new JavaSparkContext(context);
        
        //Reduce Function for sum
        Function2<Integer, Integer, Integer> reduceSumFunc = (accum, n) -> (accum + n);
        
        
        // Parallelized with 2 partitions
        JavaRDD<String> x = sc.parallelize(
                        Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b"),
                        3);
        
        // PairRDD parallelized with 3 partitions
        // mapToPair function will map JavaRDD to JavaPairRDD
        JavaPairRDD<String, Integer> rddX = 
                        x.mapToPair(e -> new Tuple2<String, Integer>(e, 1));
        
        JavaPairRDD<String, Iterable<Integer>> value=rddX.groupByKey();
        
        
       
       for(Tuple2<String, Iterable<Integer>> groupinfo: value.collect()) {
    	   System.out.println("key"+groupinfo._1);
    	 
    	   Iterable<Integer> valueList=groupinfo._2;
    	   Iterator it=valueList.iterator();
    	  while (it.hasNext()) {
			Object object = (Object) it.next();
			System.out.println("Object "+object);
		}
    	
       }
        
        // New JavaPairRDD 
        JavaPairRDD<String, Integer> rddY = rddX.reduceByKey(reduceSumFunc);
        
        //Print tuples
        for(Tuple2<String, Integer> element : rddY.collect()){
            System.out.println("("+element._1+", "+element._2+")");
        }
        
        Thread.sleep(1000000);
        String st=null;
     
    }
}

// Output:
// (b, 5)
// (a, 3)
