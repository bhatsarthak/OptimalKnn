import org.apache.spark.api.java.*;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.Accumulator;

import java.sql.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;

import com.google.common.base.Splitter;

public class OptimalKnn {
	public static void main(String[] args) {
		Integer k = 5;
		Integer d = 2;
		String datasetR = args[0];
		Integer noOfPartitions = 2;
		if(args.length > 2){
			noOfPartitions=Integer.parseInt(args[2]);
		}
		if(args.length > 3){
			k=Integer.parseInt(args[3]);
		}
		Date d1 = new Date(System.currentTimeMillis());
		long startTime = d1.getTime();
		SparkConf conf = new SparkConf().setAppName("knn");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		final Broadcast<Integer> broadcastK = ctx.broadcast(k);
		final Broadcast<Integer> broadcastD = ctx.broadcast(d);
		JavaRDD<String> R = ctx.textFile(datasetR, 1);

		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] rTokens = s.split("\\*");
				String rRecordID = rTokens[0];
				String r = rTokens[2].substring(1, rTokens[2].length() - 1);
				Double lat = Double.parseDouble(r.split(",")[0]);
				Double lon = Double.parseDouble(r.split(",")[1]);
				Double eucledian = lat * lat + lon * lon;
				String key = eucledian.toString() + ">>" + lat.toString()
						+ ">>" + lon.toString();
				return new Tuple2(key, rRecordID);
			}
		};

		JavaPairRDD<String, String> pairs = R.mapToPair(keyData);
		JavaPairRDD<String, String> sortedPairs = pairs.sortByKey();

		List<Tuple2<String, String>> sortedList = sortedPairs
				.take((int) sortedPairs.count());
		String minKey = sortedList.get(0)._1;
		String maxKey = sortedList.get(sortedList.size() - 1)._1;
		Double min = Double.parseDouble(minKey.split(">>")[0]);
		Double max = Double.parseDouble(maxKey.split(">>")[0]);
		System.out.println("MAX:" + max + "MIN:" + min);
		Double sizeOfPartition = (max - min) / noOfPartitions;
		final Broadcast<Double> broadcastMin = ctx.broadcast(min);
		final Broadcast<Double> broadcastPartitionSize = ctx
				.broadcast(sizeOfPartition);

		JavaPairRDD<Integer, Tuple2<String, String>> output = sortedPairs
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, Integer, Tuple2<String, String>>() {

					public Iterable<Tuple2<Integer, Tuple2<String, String>>> call(
							Tuple2<String, String> input) {
						List<Tuple2<Integer, Tuple2<String, String>>> partitionResult = new ArrayList<Tuple2<Integer, Tuple2<String, String>>>();
						Double distFromOrigin = Double.parseDouble(input._1
								.split(">>")[0]);
						Double minVal = broadcastMin.getValue();
						Double partitionSize = broadcastPartitionSize
								.getValue();
						Double diffFromMin = distFromOrigin - minVal;

						int currentPartition = (int) (diffFromMin / partitionSize);
						System.out.println("minVal:" + minVal
								+ "partitionSize:" + partitionSize
								+ "DiffFromMin:" + diffFromMin
								+ "CurrentPartition:" + currentPartition);
						// Add the current partition to results
						Tuple2<Integer, Tuple2<String, String>> temp = new Tuple2<Integer, Tuple2<String, String>>(
								currentPartition, input);
						partitionResult.add(temp);
						// Check if the point is in the boundry of the current
						// partition and add it if yes
						double pointPositionInPartition = diffFromMin
								% partitionSize;
						pointPositionInPartition -= (long) pointPositionInPartition;
						// Assume its very close to the previous quadrant
						if (pointPositionInPartition < 0.2) {
							if (currentPartition > 0) {

								Tuple2<Integer, Tuple2<String, String>> temp1 = new Tuple2<Integer, Tuple2<String, String>>(
										currentPartition - 1, input);
								partitionResult.add(temp1);
							}
						} else if (pointPositionInPartition > 0.7) {
							Tuple2<Integer, Tuple2<String, String>> temp2 = new Tuple2<Integer, Tuple2<String, String>>(
									currentPartition + 1, input);
							partitionResult.add(temp2);
						}
						return partitionResult;
					}

				});
		JavaPairRDD<Integer, Iterable<Tuple2<String, String>>> knnGrouped = output
				.groupByKey();

		JavaPairRDD<Integer, Tuple2<Integer, String>> finalOutput = knnGrouped
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<String, String>>>, Integer, Tuple2<Integer, String>>() {

					public Iterable<Tuple2<Integer, Tuple2<Integer, String>>> call(
							Tuple2<Integer, Iterable<Tuple2<String, String>>> locationData) {
						List<Tuple2<Integer, Tuple2<Integer, String>>> finalOutputElement = new ArrayList<Tuple2<Integer, Tuple2<Integer, String>>>();
						for (Tuple2<String, String> sourceData : locationData._2) {
							int sourcePlaceID = Integer.parseInt(sourceData._2);
							for (Tuple2<String, String> destinationData : locationData._2) {
								if (sourceData.equals(destinationData)) {
									continue;
								}
								String distanceCalcualted = calculateDistance(
										sourceData._1, destinationData._1);
								int destinationPlaceID = Integer
										.parseInt(destinationData._2);
								Tuple2<Integer,String> temp= new Tuple2<Integer,String>(destinationPlaceID, distanceCalcualted);
								Tuple2<Integer,Tuple2<Integer,String>> temp1 = new Tuple2<Integer,Tuple2<Integer,String>>(sourcePlaceID,temp);
								finalOutputElement.add(temp1);
							}
					
						}
						return finalOutputElement;

					}

				});
		
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, String>>> finalOutputGroupByKey=finalOutput.groupByKey();
		
		JavaPairRDD<Integer, Tuple2<Integer, Double>> kNearestNeighbour = finalOutputGroupByKey
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Integer, String>>>, Integer, Tuple2<Integer, Double>>() {

					public Iterable<Tuple2<Integer, Tuple2<Integer, Double>>> call(
							Tuple2<Integer, Iterable<Tuple2<Integer, String>>> input){
						SortedMap<Double, Integer> nearestK=findNearestK(input._2,broadcastK.getValue());
						
						List<Tuple2<Integer, Tuple2<Integer, Double>>> nearestKArrayList = new ArrayList<Tuple2<Integer, Tuple2<Integer, Double>>>();
						
						for (Entry<Double, Integer> entry : nearestK.entrySet()){
							Tuple2<Integer,Double> temp = new Tuple2<Integer,Double>(entry.getValue(),entry.getKey());
							nearestKArrayList.add(new Tuple2<Integer,Tuple2<Integer,Double>>(input._1, temp));
						}
						return nearestKArrayList;
					}
				});
		kNearestNeighbour.groupByKey().saveAsTextFile(args[1]);
		Date d2 = new Date(System.currentTimeMillis());
		long end = d2.getTime();
		System.out.println("Total time taken:" + (end - startTime));
	}

	static String calculateDistance(String rAsString, String sAsString) {

		String r[] = rAsString.split(">>");
		String s[] = sAsString.split(">>");
		// d is the number of dimensions in the vector
		/*
		 * if (r.size() != 6) { return Double.NaN; } if (s.size() != 6) { return
		 * Double.NaN; }
		 */
		// here we have (r.size() == s.size() == d)
		double sum = 0.0;
		for (int i = 1; i < 3; i++) {

			double difference = Double.parseDouble(r[i])
					- Double.parseDouble(s[i]);
			sum += difference * difference;
		}
		return String.valueOf(Math.sqrt(sum));
	}
	
	static SortedMap<Double, Integer> findNearestK(
			Iterable<Tuple2<Integer, String>> neighbors, int k) {
		// keep only k nearest neighbors
		SortedMap<Double, Integer> nearestK = new TreeMap<Double, Integer>();
		for (Tuple2<Integer, String> neighbor : neighbors) {
			Double distance = Double.parseDouble(neighbor._2);
			Integer destID = neighbor._1;
			nearestK.put(distance, destID);
			// keep only k nearest neighbors
			if (nearestK.size() > k) {
				// remove the last-highest-distance neighbor from nearestK
				nearestK.remove(nearestK.lastKey());
			}
		}
		return nearestK;
	}
	
}
