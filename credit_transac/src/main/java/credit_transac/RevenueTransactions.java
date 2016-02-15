package credit_transac;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RevenueTransactions {
	public static void main(String[] args) throws ClassNotFoundException {
	
	
final File resultsFile = new File("subsription-results.txt");
		
		SparkConf conf = new SparkConf().setAppName("deduplicatePosts").setMaster("local").registerKryoClasses(new Class<?>[]{
				Class.forName("org.apache.hadoop.io.LongWritable"),
				Class.forName("org.apache.hadoop.io.Text")
		});
		
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> transRDD = sc.textFile("subscription_report.csv");
		
		JavaPairRDD<String, String> subscriptionsRDD = transRDD.mapToPair(new PairFunction<String, String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String line) throws Exception {

				String[] fields = line.split(",");
				
				String date = fields[3];
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
				Date date1 = dateFormat.parse(date);
				String year = String.valueOf(date1.getYear()+1900);
				
				return new Tuple2<String, String>(year, fields[2]);
			}
		
		});
		
		subscriptionsRDD.saveAsTextFile("subscriptions-year-wise");
		
		
		JavaPairRDD<String, String> revenuesRDD = subscriptionsRDD.reduceByKey(new Function2<String, String, String>() {
			
			public String call(String v1, String v2) throws Exception {
				Integer sum = Integer.parseInt(v1) + Integer.parseInt(v2);
				
				
				return String.valueOf(sum);
			}
		});
		revenuesRDD = revenuesRDD.sortByKey();
		revenuesRDD.saveAsTextFile("subscriptions-year-wise-grouped");
		
	
	}
	
	
}
