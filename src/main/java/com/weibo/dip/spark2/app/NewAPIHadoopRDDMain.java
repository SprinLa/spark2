/**
 * 
 */
package com.weibo.dip.spark2.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * @author yurun
 *
 */
public class NewAPIHadoopRDDMain {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		JavaSparkContext sc = new JavaSparkContext(conf);

		Configuration hadoopConf = new Configuration();

		/*
		 * /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/
		 */
		hadoopConf.set(FileInputFormat.INPUT_DIR, "/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/");
		hadoopConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");

		JavaPairRDD<LongWritable, Text> datas = sc.newAPIHadoopRDD(hadoopConf, TextInputFormat.class,
				LongWritable.class, Text.class);

		JavaRDD<String> lines = datas.map(new Function<Tuple2<LongWritable, Text>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<LongWritable, Text> data) throws Exception {
				return data._2().toString();
			}

		});

		JavaRDD<Integer> distData = lines.map(new Function<String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String line) throws Exception {
				return 1;
			}

		});

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
