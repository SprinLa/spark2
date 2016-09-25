/**
 * 
 */
package com.weibo.dip.spark2.app;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author yurun
 *
 */
public class CollectAndForeachMain {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

		JavaRDD<Integer> distData = sc.parallelize(data);

		distData.collect().forEach(new Consumer<Integer>() {

			@Override
			public void accept(Integer t) {
				System.out.println(t);
			}

		});

		sc.close();
	}

}
