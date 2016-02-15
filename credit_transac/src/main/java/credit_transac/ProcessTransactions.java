package credit_transac;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ProcessTransactions {
	
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
				
				return new Tuple2<String, String>(fields[1], fields[3]);
			}
		
		});
		
		JavaPairRDD<String, Iterable<String>> subsGroupedRDD = subscriptionsRDD.groupByKey();
		
		JavaPairRDD<String, String> finalRDD = subsGroupedRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
		/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {

			ArrayList<String> subscrList =new ArrayList<String>(); 
			subscrList.addAll((Collection<? extends String>) tuple._2());
			FileUtils.writeStringToFile(resultsFile, "\n"+"subsription list for id "+tuple._1()+"\n", true);
			FileUtils.writeStringToFile(resultsFile, subscrList+"\n", true);
			FileUtils.writeStringToFile(resultsFile, "subscription for " + tuple._1() +" is "+"\n",true);

			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			String subscriptionType=null;
			String duration = null;
			
			if(subscrList.size() > 1){
				
				Date date1 = dateFormat.parse(subscrList.get(0));
				Date date2 = dateFormat.parse(subscrList.get(1));
				Date date3 = dateFormat.parse(subscrList.get(2));
				
				Long interval1 = date3.getTime() - date2.getTime();
				Long interval2 = date2.getTime() - date1.getTime();
				
					Integer diff = (int) ((interval2 + interval1) / (2*1000*60*60*24));
					if(diff < 7){
						FileUtils.writeStringToFile(resultsFile, "daily "+"\n", true);
						subscriptionType="daily";
					}else if(diff < 31){
						FileUtils.writeStringToFile(resultsFile, "monthly "+"\n", true);
						subscriptionType = "monthly";
					}else{
						FileUtils.writeStringToFile(resultsFile, "yearly "+"\n", true);
						subscriptionType = "yearly";
					}
					
					
					Date date4 = dateFormat.parse(subscrList.get(0));
					Date date5 = dateFormat.parse(subscrList.get(subscrList.size()-1));
					
					Long interval3 = date5.getTime() - date4.getTime();
					
						Integer diff1 = (int) ((interval3) / (1000*60*60*24));
						if(diff1 < 27){
							FileUtils.writeStringToFile(resultsFile, "duration is "+diff1+" day(s)"+"\n",true);
							duration = diff1 + "day (s)";
						}else if(diff1 < 300){
							FileUtils.writeStringToFile(resultsFile, "duration is "+diff1/30+" month(s)"+"\n",true);
							duration = (diff1/30) + " month (s)";
						}else{
							FileUtils.writeStringToFile(resultsFile, "duration is "+diff1/365+" year(s)"+"\n",true);
							duration = (diff1/365) + " year (s)";
						}
						
					
				
			}else{
				FileUtils.writeStringToFile(resultsFile, "one-off "+"\n", true);
				subscriptionType = "one-off";
			}
			if(duration != null)
				return new Tuple2<String, String>(tuple._1(), subscriptionType+","+duration);
			else
				return new Tuple2<String, String>(tuple._1(), subscriptionType);
		}
		
		});
//		subsGroupedRDD.saveAsTextFile("output-list" + new Random().nextInt());
		finalRDD.saveAsTextFile("output" + new Random().nextInt());
//		finalRDD.saveAsTextFile("output-time" + new Random().nextInt());
	}
	

}
