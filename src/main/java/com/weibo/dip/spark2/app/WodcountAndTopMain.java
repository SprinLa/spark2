/**
 * 
 */
package com.weibo.dip.spark2.app;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * @author yurun
 *
 */
public class WodcountAndTopMain {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		JavaSparkContext sc = new JavaSparkContext(conf);

		Configuration hadoopConf = new Configuration();

		/*
		 * /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/
		 * 
		 * /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_09_23/
		 * 00/www_sinaedgeahsolci14ydn_trafficserver-yf235028.scribe.dip.sina.
		 * com.cn_16469-2016_09_23_00-20160923011_00000
		 */
		hadoopConf.set(FileInputFormat.INPUT_DIR,
				"/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_09_23/00/www_sinaedgeahsolci14ydn_trafficserver-yf235028.scribe.dip.sina.com.cn_16469-2016_09_23_00-20160923011_00000");
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

		String regex = "^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$";

		Pattern pattern = Pattern.compile(regex);

		JavaPairRDD<String, Integer> pairs = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
				List<Tuple2<String, Integer>> tuples = new ArrayList<>();

				Matcher matcher = pattern.matcher(line);

				if (matcher.matches()) {
					for (int index = 1; index <= matcher.groupCount(); index++) {
						tuples.add(new Tuple2<String, Integer>(matcher.group(index), new Integer(1)));
					}
				}

				return tuples.iterator();
			}

		}).filter(new Function<Tuple2<String, Integer>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
				return tuple != null && StringUtils.isNotEmpty(tuple._1());
			}

		});

		// JavaPairRDD<String, Integer> wordcounts = pairs.reduceByKey(new
		// Function2<Integer, Integer, Integer>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Integer call(Integer x, Integer y) throws Exception {
		// return x + y;
		// }
		//
		// });

		System.out.println(pairs.count());

		// List<Tuple2<String, Integer>> tops = wordcounts
		// .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer,
		// String>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple)
		// throws Exception {
		// return new Tuple2<Integer, String>(tuple._2(), tuple._1());
		// }
		//
		// }).sortByKey().mapToPair(new PairFunction<Tuple2<Integer, String>,
		// String, Integer>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple)
		// throws Exception {
		// return new Tuple2<String, Integer>(tuple._2(), tuple._1());
		// }
		//
		// }).take(10);
		//
		// tops.forEach(new Consumer<Tuple2<String, Integer>>() {
		//
		// @Override
		// public void accept(Tuple2<String, Integer> tuple) {
		// System.out.println(tuple._1() + "\t" + tuple._2());
		// }
		//
		// });

		sc.close();
	}

}
