import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.sql.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
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

		Date d1 =  new Date(System.currentTimeMillis());
		long startTime=d1.getTime();
		SparkConf conf = new SparkConf().setAppName("knn");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		final Broadcast<Integer> broadcastK = ctx.broadcast(k);
		final Broadcast<Integer> broadcastD = ctx.broadcast(d);
		JavaRDD<String> R = ctx.textFile(datasetR, 1);
		/*		JavaRDD<String> squareOfR =R.map(new Function<String, String>() {
			  public String call(String s) { 
				    String[] rTokens = s.split("\\*");
					String rRecordID = rTokens[0];
					String r = rTokens[2].substring(1,
							rTokens[2].length() - 1);
					Float lat = Float.parseFloat(r.split(",")[0]);
					Float lon = Float.parseFloat(r.split(",")[1]);
					Float eucledian = lat * lat + lon * lon;
				    return eucledian.toString() +">>"+lat.toString() +">>"+lon.toString() + ">>"+ rRecordID; }
			});*/	
		PairFunction<String, String, String> keyData =
				new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] rTokens = s.split("\\*");
				String rRecordID = rTokens[0];
				String r = rTokens[2].substring(1,
						rTokens[2].length() - 1);
				Float lat = Float.parseFloat(r.split(",")[0]);
				Float lon = Float.parseFloat(r.split(",")[1]);
				Float eucledian = lat * lat + lon * lon;
				String key = eucledian.toString() +">>"+lat.toString() +">>"+lon.toString();
				return new Tuple2(key, rRecordID);
			}
		};
		JavaPairRDD<String, String> pairs = R.mapToPair(keyData);
		JavaPairRDD<String, String> sortedPairs = pairs.sortByKey();
		//sortedPairs.saveAsTextFile(args[1]);
		List<Tuple2<String,String>> sortedList = new LinkedList<Tuple2<String,String>>();
		sortedList = sortedPairs.collect();
		for(int i =0;i < sortedList.size();i++){
			int start = (k<i) ?(i-k) :0;
			System.out.println("Start:"+start);
			int max = sortedList.size();
			System.out.println("MaxVal"+max);
			int end = (i+k>(max-1))?max-1:(i+k);
			System.out.println("End:"+end);
			String sourcePlaceId=sortedList.get(i)._2;
			String sourceData =sortedList.get(i)._1;
            System.out.println("The source is :"+sourceData);
			TreeSet <String> ts = new TreeSet<String>();
			String[] tsToArray = new String[ts.size()];
			for(int j=start;j<=end;j++){						
						String destPlaceId=sortedList.get(j)._2;					
						String destData= sortedList.get(j)._1;
						String distance = calculateDistance(sourceData,destData);
						ts.add(distance + ">>" + String.valueOf(j));
			}
			
			ts.toArray(tsToArray);
			System.out.println("The nearest neighbour is:" +ts.size());
			for(String element:ts) {
				int index = Integer.parseInt(element.split(">>")[1]);
				System.out.println( sortedList.get(index)._1);
				
			}
/*			for(int j=0;j< tsToArray.length;j++){							
				int index = Integer.parseInt(tsToArray[j].split(">>")[1]);
				System.out.println(String.valueOf(index));
				System.out.println( sortedList.get(index)._1);
				System.out.println(tsToArray[j]);
			}*/
		}
		Date d2 =  new Date(System.currentTimeMillis());
		long end=d2.getTime();
		System.out.println("Total time taken:"+ (end-startTime));
	}

	static String calculateDistance(String rAsString, String sAsString) {

		String r[] = rAsString.split(">>");
		String s[] = sAsString.split(">>");
		// d is the number of dimensions in the vector
		/*
		 * if (r.size() != 6) { return Double.NaN; } if (s.size() != 6) {
		 * return Double.NaN; }
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
}

