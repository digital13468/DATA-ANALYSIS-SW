import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;



public class repoInfo {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: Repo Info <input> <output>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("Repo Info with Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> githubLine = context.textFile(args[0]);
		
		JavaPairRDD<String, String> github = githubLine.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String ,String> call(String s) throws Exception {
				String[] tokens = s.split(",");
				String repo = tokens[0];
				String lang = tokens[1];
				String star = tokens[12];
				
				String value = repo + "," + star + ",1";
				return new Tuple2<String, String>(lang, value);
			}
		});
		
		JavaPairRDD<String, String> lang = github.reduceByKey(new Function2<String, String, String>() {
			public String call(String s1, String s2) {
				String[] tokens1 = s1.split(",");
				String[] tokens2 = s2.split(",");
				
				if (Integer.parseInt(tokens1[1]) > Integer.parseInt(tokens2[1]))
					return tokens1[0] + "," + tokens1[1] + "," + (Integer.parseInt(tokens1[2]) + Integer.parseInt(tokens2[2]));
				else
					return tokens2[0] + "," + tokens2[1] + "," + (Integer.parseInt(tokens1[2]) + Integer.parseInt(tokens2[2]));
			}
		});
		
		JavaPairRDD<Integer, String> numOfRepo = lang.mapToPair(new PairFunction<Tuple2<String, String>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, String> tuple) {
				String[] tokens = tuple._2.split(",");
				String lang = tuple._1;
				String numOfRepo = tokens[2];
				String nameOfRepoHighestStar = tokens[0];
				String numStars = tokens[1];
				
				return new Tuple2<Integer, String>(Integer.parseInt(numOfRepo), lang + " " + numOfRepo + " " + nameOfRepoHighestStar + " " + numStars); 
			}
		});
		
		numOfRepo.sortByKey(false).flatMap(new FlatMapFunction<Tuple2<Integer, String>, String>() {
			public Iterable<String> call(Tuple2<Integer, String> tuple) {
				List<String> returnValues = new ArrayList<String>();
				returnValues.add(tuple._2);
				return returnValues;
			}
		}).coalesce(1).saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}
