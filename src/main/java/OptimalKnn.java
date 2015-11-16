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
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;

import com.google.common.base.Splitter;

public class OptimalKnn {
	public static void main(String[] args) {
		Integer k = 5;
		Integer d = 2;
		String datasetR = args[0];

		Date d1 =  new Date(System.currentTimeMillis());
		long start=d1.getTime();
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
				sortedPairs.saveAsTextFile("/home/sarthakbhat/workspace/OptimalRetailStorePlacement/output");
	}
}
