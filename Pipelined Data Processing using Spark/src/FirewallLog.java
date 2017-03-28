
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FirewallLog {

	private final static int numOfReducers = 6;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage: Firewall Log <ip_trace> <raw_block> <log_output> <sorted_output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Firewall Logs with Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> ipTraceLine = context.textFile(args[0]);
		JavaRDD<String> rawBlockLine = context.textFile(args[1]);

		JavaPairRDD<String,String> ipTrace = ipTraceLine.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s");
				String time = tokens[0];
				String connection_id = tokens[1];
				String source_ip = tokens[2];
				String destination_ip = tokens[4];
				//String protocol = tokens[5];
				//String protocol_dependent_data = tokens[6];
				
				String value = time + " "+ connection_id + " " + source_ip + " " + destination_ip;
				return new Tuple2<String, String>(connection_id, value);
			}
		});
		
		JavaPairRDD<String,String> rawBlock = rawBlockLine.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s");
				String connection_id = tokens[0];
				String action_taken = tokens[1];

				return new Tuple2<String, String>(connection_id, action_taken);
			}
		}).cache().filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				String value = tuple._2;
				
				if (value.equals("Blocked")) {
					return true;
				}
				return false;
			}
		});
		
		JavaPairRDD<String, Tuple2<String, String>> firewallLog = ipTrace
				.join(rawBlock, numOfReducers);
		
		JavaRDD<String> firewallLogFile = firewallLog.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<String, String>>, String>() {
			public Iterable<String> call(Tuple2<String, Tuple2<String, String>> tuple) {
				List<String> returnValues = new ArrayList<String>();
				returnValues.add(tuple._2._1 + " " + tuple._2._2);
				return returnValues;
			}
		});
		firewallLogFile.repartition(1).saveAsTextFile(args[2]);
		//firewallLogFile.saveAsTextFile(args[2]);
		JavaPairRDD<String, Integer> blockOnes = firewallLogFile.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] tokens = s.split("\\s");
				String source_ip = tokens[2];
				return new Tuple2<String, Integer>(source_ip, 1);
			}
		});
		
		JavaPairRDD<String, Integer> blockCounts = blockOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);
		
		JavaPairRDD<Integer, String> countIP = blockCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) {
				return new Tuple2<Integer, String>(tuple._2, tuple._1);
			}
		});
		
		
		countIP.sortByKey(false).coalesce(1).saveAsTextFile(args[3]);
		context.stop();
		context.close();
		
	}
}