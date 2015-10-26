import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;

import com.google.common.base.Splitter;

public class SimpleApp {
	public static void main(String[] args) {
		try {
			Integer k = 5;
			Integer d = 2;
			String datasetR = "/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/sample.txt";
			String datasetS = "/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/sample.txt";
			SparkConf conf = new SparkConf().setAppName("knn");
			JavaSparkContext ctx = new JavaSparkContext(conf);
			final Broadcast<Integer> broadcastK = ctx.broadcast(k);
			final Broadcast<Integer> broadcastD = ctx.broadcast(d);
			JavaRDD<String> R = ctx.textFile(datasetR, 1);
			JavaRDD<String> S = ctx.textFile(datasetS, 1);
			JavaPairRDD<String, String> cart = R.cartesian(S);
			JavaPairRDD<String, Tuple2<Double, String>> knnMapped = cart
					.mapToPair(new PairFunction<Tuple2<String, String>, // input
					String,
					// K
					Tuple2<Double, String> // V
					>() {
						public Tuple2<String, Tuple2<Double, String>> call(
								Tuple2<String, String> cartRecord) {
							try {
								String rRecord = cartRecord._1;
								String sRecord = cartRecord._2;
								String[] rTokens = rRecord.split("\\*");

								String rRecordID = rTokens[0];
								
								String r =  rTokens[2].substring(1,
								rTokens[2].length() - 1);// r.1, r.2, ...,
								
								String[] sTokens = sRecord.split("\\*");
								// sTokens[0] = s.recordID
								String sClassificationID = sTokens[0];
								String s = sTokens[2].substring(1,
										sTokens[2].length() -1); // s.1, s.2, ...,
												// s.d
								Integer d = broadcastD.value();
								double distance = calculateDistance(r, s, d);
								String K = rRecordID; // r.recordID
								Tuple2<Double, String> V = new Tuple2<Double, String>(
										distance, sClassificationID);
								return new Tuple2<String, Tuple2<Double, String>>(
										K, V);
							} catch (ArrayIndexOutOfBoundsException e) {

							} catch (Exception e) {
								System.out.println("Inner exception caught");
							}
							return null;
						}
					});
			knnMapped.saveAsTextFile("/home/sarthakbhat/output/knnMapped");
			JavaPairRDD<String, Iterable<Tuple2<Double,String>>> knnGrouped =
					knnMapped.groupByKey();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static double calculateDistance(String rAsString, String sAsString, int d) {
		try {
			String r[] = rAsString.split(",");
			String s[] = sAsString.split(",");
			// d is the number of dimensions in the vector
/*			if (r.size() != 6) {
				return Double.NaN;
			}
			if (s.size() != 6) {
				return Double.NaN;
			}*/
			// here we have (r.size() == s.size() == d)
			double sum = 0.0;
			for (int i = 0; i < d -1; i++) {
				
				double difference = Double.parseDouble(r[i]) - Double.parseDouble(s[i]);
				sum += difference * difference;
			}
			return Math.sqrt(sum);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	static List<Double> splitOnToListOfDouble(String str, String delimiter) {

		String[] latLong = str.split(delimiter);
		List<Double> list = new ArrayList<Double>();
		for (String token : latLong) {
			double data = Double.parseDouble(token);
			list.add(data);
		}
		return list;
		/*
		 * Splitter splitter = Splitter.on(delimiter).trimResults();
		 * Iterable<String> tokens = splitter.split(str); if (tokens == null) {
		 * return null; } List<Double> list = new ArrayList<Double>(); for
		 * (String token: tokens) { double data = Double.parseDouble(token);
		 * list.add(data); } return list;
		 */
	}
	
	static SortedMap<Double, String> findNearestK(
			Iterable<Tuple2<Double,String>> neighbors,
			int k) {
			// keep only k nearest neighbors
			SortedMap<Double, String> nearestK = new TreeMap<Double, String>();
			for (Tuple2<Double,String> neighbor : neighbors) {
			Double distance = neighbor._1;
			String classificationID = neighbor._2;
			nearestK.put(distance, classificationID);
			// keep only k nearest neighbors
			if (nearestK.size() > k) {
			// remove the last-highest-distance neighbor from nearestK
			nearestK.remove(nearestK.lastKey());
			}
			}
			return nearestK;
			}

}
