/**
 * 
 */
package com.weibo.dip.spark2.app;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @author yurun
 *
 */
public class BasicMain2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

		JavaRDD<Integer> distData = sc.parallelize(data);

		Integer result = distData.reduce(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer x, Integer y) throws Exception {
				return x + y;
			}

		});

		System.out.println("result: " + result);

		sc.close();
	}

}
