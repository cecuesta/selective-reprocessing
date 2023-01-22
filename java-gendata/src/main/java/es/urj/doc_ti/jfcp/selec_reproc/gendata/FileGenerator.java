
package es.urj.doc_ti.jfcp.selec_reproc.gendata ;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.datafaker.formats.Csv;
import net.datafaker.formats.Format;

@SuppressWarnings("deprecation")
public class FileGenerator {

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	static Date from = null ;

	public static void main(String[] args) {
		
		MyCustomFaker myFaker = new MyCustomFaker();

		try {
			from = sdf.parse("2023-12-15");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Date to = new Date( from.getTime() + 24*3600*1000 - 1000);
	
		
		System.out.println(
		        Format.toCsv(
//		                Csv.Column.of("first_name", () -> myFaker.name().firstName()),
//		                Csv.Column.of("last_name", () -> myFaker.name().lastName()),
//		                Csv.Column.of("address", () -> myFaker.address().streetAddress()),
//		                Csv.Column.of("xxx", () -> myFaker.phoneNumber().subscriberNumber(9)),
//		                Csv.Column.of("xxx", () -> myFaker.myNumbers().nextInsectName() ),
		                Csv.Column.of("xxx", () -> myFaker.MyElements().nextDeterminedDistribElement("id1") ),
		                Csv.Column.of("xxx", () -> myFaker.MyElements().exponentialDistributedNumber(10, 4) ),
		                Csv.Column.of("xxx", () -> myFaker.date().between(from, to, "yyyy-MM-dd'T'HH:mm:ss") ) 
		                
		                )
		            .header(false)
		            .separator(",")
		            .limit(20).build().get());

		
		double sum = 0 ;
		double sum2 = 0 ;
		int MAX = 10000 ;
		for (int n = 0 ; n< MAX;n++ ) {
			double aux = myFaker.MyElements().exponentialDistributedNumberAsDouble(100) ;
			sum +=  aux;
			sum2 +=  aux*aux;
		}
		
		double media = sum / MAX ;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
		double desv = sum2/MAX - media*media ; 
		System.out.println ( media ) ;
		System.out.println ( desv ) ;
		
		
	}
	
}
