package credit_transac;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;

public class Test {

	public static void main(String[] args) throws ParseException {
		
		String str = "04/25/1970";
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
//		System.out.println(dateFormat.parse(str));
//		System.out.println(dateFormat.parse(str).getDate());
//		System.out.println(dateFormat.parse(str).getDay());
//		System.out.println(dateFormat.parse(str).getMonth());
		System.out.println(dateFormat.parse(str).getYear());
		
		/*
		Date date1 = dateFormat.parse("10/15/1983");
		Date date2 = dateFormat.parse("11/15/1983");
		Date date3 = dateFormat.parse("12/15/1983");
		
		Long interval1 = date3.getTime() - date2.getTime();
		Long interval2 = date2.getTime() - date1.getTime();
		
			Integer diff = (int) ((interval2 + interval1) / (2*86400000));
			if(diff < 5){
				System.out.println("daily "+"\n");
			}else if(diff < 30){
				System.out.println("monthly "+"\n");
			}else{
				System.out.println("yearly "+"\n");
			}
		*/
	}
	
}
