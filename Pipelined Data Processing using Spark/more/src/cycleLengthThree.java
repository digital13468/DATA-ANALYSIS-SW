import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class cycleLengthThree {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		String input = null;
		String directedCycle = null;
		String totalNumber = null;
		if (args.length < 2 || args.length > 3) {
			System.err.println("Usage: Cycle of Length Three <input> <output>");
			System.exit(1);
		}
		else if (args.length == 2) {
			input = args[0];
			totalNumber = args[1];
		}
		else if (args.length == 3) {
			input = args[0];
			totalNumber = args[1];
			directedCycle = args[2];
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("Directed Cycle of Length Three with Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> patentsLine = context.textFile(input);
		
		JavaPairRDD<String, String> patents = patentsLine.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String ,String> call(String s) throws Exception {
				String[] tokens = s.split("[\\s]+");
				if (tokens[0] != tokens[1]) 
					return new Tuple2<String, String>(tokens[0], tokens[1]);
				else
					return new Tuple2<String, String>(null, null);
			}
		});
		/*
		JavaRDD<String> patent = patents.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
			public Iterable<String> call(Tuple2<String, String> tuple) {
				List<String> returnValues = new ArrayList<String>();
				returnValues.add(tuple._1 + ">" + tuple._2);
				returnValues.add(tuple._2 + "<" + tuple._1);
				return returnValues;
			}
		});*/
		JavaPairRDD<String, String> patent = patents.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
			public Iterable<Tuple2<String, String>> call(Tuple2<String, String> tuple) {
				List<Tuple2<String, String>> returnValues = new ArrayList<Tuple2<String, String>>();
				returnValues.add(new Tuple2<String, String>(tuple._1, ">" + tuple._2));
				returnValues.add(new Tuple2<String, String>(tuple._2, "<" + tuple._1));
				return returnValues;
			}
		});/*
		JavaPairRDD<String, String> degree = patent.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				if (s.contains(">")) {
					String[] tokens = s.split(">");
					return new Tuple2<String, String>(tokens[0], ">" + tokens[1]);
				}
				else {
					String[] tokens = s.split("<");
					return new Tuple2<String, String>(tokens[0], "<" + tokens[1]);
				}
			}
		});*/
		
		JavaPairRDD<String, String> cycle = patent.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) {
				String node = tuple._1;
				Set<String> outgoing = new HashSet<String>();
				Set<String> incoming = new HashSet<String>();
				List<Tuple2<String, String>> returnValues = new ArrayList<Tuple2<String, String>>();
				
				for (String neighborhood : tuple._2) {
					 String direction = neighborhood.substring(0, 1);
					 String neighbor = neighborhood.substring(1);
					 //returnValues.add(new Tuple2<String, String>(node, direction + neighbor));
					 if (direction.equals(">"))
						 outgoing.add(neighbor);
					 else 
						 incoming.add(neighbor);
					
				}
				for (String out : outgoing) {
					if (Integer.parseInt(out) < Integer.parseInt(node)) 
						returnValues.add(new Tuple2<String, String>(out, "<" + node));
					for (String in : incoming) {
						if ((Integer.parseInt(in) < Integer.parseInt(node)) && (Integer.parseInt(in) < Integer.parseInt(out)) && (Integer.parseInt(in) != Integer.parseInt(out)))
							returnValues.add(new Tuple2<String ,String>(in, ">" + node + ">" + out));
					}
				}
				return returnValues;
			}
		});
		
		JavaPairRDD<String, Integer> three = cycle.groupByKey().mapValues(new Function<Iterable<String>, Integer>() {
			public Integer call(Iterable<String> directed) {
				Set<String> from = new HashSet<String>();
				Set<String> to = new HashSet<String>();
				for (String directedCycle : directed) {
					String direction = directedCycle.substring(0, 1);
					if (direction.equals("<"))
						from.add(directedCycle.substring(1));
					else 
						to.add(directedCycle);
				}
				int count = 0;
				for (String lengthTwo : to) {
					if (from.contains(lengthTwo.substring(lengthTwo.lastIndexOf(">") + 1)))
						count ++;
				}
				return count;
			}
		}); 
		
		if (directedCycle != null)
			three.coalesce(1).saveAsTextFile(directedCycle);
		/*
		Tuple2<String, Integer> total = three.reduce(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
				return new Tuple2<String, Integer>("total", tuple1._2 + tuple2._2);
			}
		});*/
		
		JavaPairRDD<String, Integer> total = three.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) {
				return new Tuple2<String, Integer>("total", tuple._2); 
			}
		});
		
		JavaPairRDD<String, Integer> number = total.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		number.coalesce(1).saveAsTextFile(totalNumber);
		context.stop();
		context.close();
	}
}
