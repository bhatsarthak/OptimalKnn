import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.sql.Date;
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
			String datasetR = args[0];// "/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/newyork_locdate.txt";
			String datasetS = args[0];// "/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/newyork_locdate.txt";
			// String datasetR =
			// "/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/sample.txt";
			// String datasetS =
			// "/home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/sample.txt";

			Date d1 = new Date(System.currentTimeMillis());
			long start = d1.getTime();
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

								String r = rTokens[2].substring(1,
										rTokens[2].length() - 1);// r.1, r.2,
																	// ...,

								String[] sTokens = sRecord.split("\\*");
								// sTokens[0] = s.recordID
								String sClassificationID = sTokens[0];
								String s = sTokens[2].substring(1,
										sTokens[2].length() - 1); // s.1, s.2,
																	// ...,
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

			
			/*  JavaPairRDD<String, Iterable<Tuple2<Double, String>>> knnGrouped
			  = knnMapped .groupByKey(); JavaPairRDD<String, String> knnOutput
			  = knnGrouped .mapValues(new Function<Iterable<Tuple2<Double,
			  String>>, // input String // output (classification) >() { public
			  String call( Iterable<Tuple2<Double, String>> neighbors) {
			  Integer k = broadcastK.value(); SortedMap<Double, String>
			  nearestK = findNearestK( neighbors, k); String
			  selectedClassification=""; for (Map.Entry<Double, String> entry :
			  nearestK.entrySet()) { selectedClassification =
			  selectedClassification + "NeighbourID:"+ entry.getValue()
			  +"+++NeighbourDistance" +entry.getKey().toString()+"\n"; } return
			  selectedClassification; } });*/
			 
			Function<Tuple2<Double, String>, List<Tuple2<Double,String>>> createCombiner = new Function<Tuple2<Double, String>, List<Tuple2<Double,String>>>() {
				public List<Tuple2<Double,String>> call(Tuple2<Double, String> input)
						throws Exception {
					List<Tuple2<Double,String>> li = new ArrayList<Tuple2<Double,String>>();
					li.add(input);
					return li;
				}
			};
			Function2<List<Tuple2<Double,String>>, Tuple2<Double, String>, List<Tuple2<Double,String>>> mergeValue = new Function2<List<Tuple2<Double,String>>, Tuple2<Double, String>,List<Tuple2<Double,String>>>() {
				public List<Tuple2<Double,String>> call(List<Tuple2<Double,String>> a, Tuple2<Double, String> x) {
						a.add(x);
						return a;
					// left as an exercise to an interested reader
				}
			};

			Function2<List<Tuple2<Double, String>>, List<Tuple2<Double, String>>, List<Tuple2<Double, String>>> mergeCombiners = new Function2<List<Tuple2<Double,String>>,List<Tuple2<Double,String>>, List<Tuple2<Double,String>>>() {
				public List<Tuple2<Double,String>> call(List<Tuple2<Double,String>> a,List<Tuple2<Double,String>> b) {
					//Add the contents to the hashmap, if size exceeds k then remove the data
/*					System.out.println("A Size"+a.size()+"B Size"+b.size());
					SortedMap<Double, String> nearestK = new TreeMap<Double, String>();
					for(Tuple2<Double,String> element:a){
						nearestK.put(element._1, element._2);
						if(nearestK.size() > broadcastK.getValue()){
							nearestK.remove(nearestK.lastKey());
						}
					}
					
					for(Tuple2<Double,String> element:b){
						nearestK.put(element._1, element._2);
						if(nearestK.size() > broadcastK.getValue()){
							nearestK.remove(nearestK.lastKey());
						}
					}
					List<Tuple2<Double,String>> finalOutput = new ArrayList<Tuple2<Double,String>>();

					for(Map.Entry<Double, String> entry: nearestK.entrySet()){
						finalOutput.add(new Tuple2<Double,String>(entry.getKey(),entry.getValue()));
					}
					return null;*/
					// left as an exercise to an interested reader
					for(Tuple2<Double,String> element:b){
						a.add(new Tuple2<Double,String>(element._1, element._2));
					}
					return a;
				}
			};

			JavaPairRDD<String, List<Tuple2<Double, String>>> knnOutput = knnMapped.combineByKey(
					createCombiner, mergeValue, mergeCombiners);
			knnOutput.map(new Function<Tuple2<String,List<Tuple2<Double,String>>>, String>(){

				public String call(
						Tuple2<String, List<Tuple2<Double, String>>> input)
						throws Exception {
					String selectedClassification="SourceID:"+input._1+"\n";
					SortedMap<Double, String> nearestK = new TreeMap<Double, String>();
					for(Tuple2<Double,String> element:input._2){
						nearestK.put(element._1, element._2);
						if(nearestK.size() > broadcastK.getValue()){
							nearestK.remove(nearestK.lastKey());
						}
						
					}
					
					for(Map.Entry<Double, String> entry: nearestK.entrySet()){
						 selectedClassification =
								  selectedClassification + "NeighbourID:"+ entry.getValue()
								  +"+++NeighbourDistance" +entry.getKey().toString()+"\n";
					}
					// TODO Auto-generated method stub
					return selectedClassification;
				}
				
			}).saveAsTextFile(args[1]);
			Date d2 = new Date(System.currentTimeMillis());
			long end = d2.getTime();
			System.out.println("Total time taken:" + (end - start));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static double calculateDistance(String rAsString, String sAsString, int d) {
		try {
			String r[] = rAsString.split(",");
			String s[] = sAsString.split(",");
			// d is the number of dimensions in the vector
			/*
			 * if (r.size() != 6) { return Double.NaN; } if (s.size() != 6) {
			 * return Double.NaN; }
			 */
			// here we have (r.size() == s.size() == d)
			double sum = 0.0;
			for (int i = 0; i < d - 1; i++) {

				double difference = Double.parseDouble(r[i])
						- Double.parseDouble(s[i]);
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
			Iterable<Tuple2<Double, String>> neighbors, int k) {
		// keep only k nearest neighbors
		SortedMap<Double, String> nearestK = new TreeMap<Double, String>();
		for (Tuple2<Double, String> neighbor : neighbors) {
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
