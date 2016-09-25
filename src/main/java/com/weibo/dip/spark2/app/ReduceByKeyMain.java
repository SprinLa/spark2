/**
 * 
 */
package com.weibo.dip.spark2.app;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author yurun
 *
 */
public class ReduceByKeyMain {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> data = Arrays.asList("a", "b", "c", "a", "b", "a");

		JavaRDD<String> distData = sc.parallelize(data);

		JavaPairRDD<String, Integer> pairs = distData.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});

		JavaPairRDD<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer x, Integer y) throws Exception {
				return x + y;
			}

		});

		wordcounts.take(10).forEach(new Consumer<Tuple2<String, Integer>>() {

			@Override
			public void accept(Tuple2<String, Integer> t) {
				System.out.println(t._1() + "\t" + t._2());
			}

		});

		sc.close();
	}

}
