
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
		        		
//		                Csv.Column.of("f", () -> myFaker.MyElements().nextDeterminedDistribElement("id3")),
//		                Csv.Column.of("f", () -> myFaker.MyElements().nextDeterminedDistribElement("id3")),
		                Csv.Column.of("f3", () -> myFaker.MyElements().nextDeterminedDistribElement("id3")),
		                Csv.Column.of("f4", () -> myFaker.MyElements().nextDeterminedDistribElement("id4")),
		                Csv.Column.of("f5", () -> myFaker.MyElements().nextDeterminedDistribElement("id5")),
		                Csv.Column.of("f6", () -> myFaker.MyElements().exponentialDistributedNumber(500, 10000, 0) ),
		                Csv.Column.of("f7", () -> myFaker.MyElements().nextDeterminedDistribElement("id7")),
		                Csv.Column.of("f8", () -> myFaker.MyElements().nextDeterminedDistribElement("id8")),
		                Csv.Column.of("f9", () -> myFaker.MyElements().nextDeterminedDistribElement("id9")),
		                Csv.Column.of("f10", () -> myFaker.MyElements().nextDeterminedDistribElement("id10")),
		                Csv.Column.of("f11", () -> myFaker.expression("#{numerify '###'}")) ,
		                Csv.Column.of("f12", () -> myFaker.expression("#{numerify '##'}")) ,
		                Csv.Column.of("f13", () -> myFaker.expression("#{numerify '###'}")) ,
		                Csv.Column.of("f14", () -> myFaker.expression("#{numerify '###'}")) ,
		                Csv.Column.of("f15", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f16", () -> myFaker.expression("#{numerify '###'}")) ,
		                Csv.Column.of("f17", () -> myFaker.expression("#{numerify '####'}")) ,
		                Csv.Column.of("f18", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f19", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f20", () -> myFaker.expression("#{numerify '##'}")) ,
		                Csv.Column.of("f21", () -> myFaker.expression("#{numerify '###'}")) ,
		                Csv.Column.of("f22", () -> myFaker.MyElements().exponentialDistributedNumber(20, 200, 0) ),
		                Csv.Column.of("f23", () -> myFaker.MyElements().exponentialDistributedNumber(5, 99, 0) ),
		                Csv.Column.of("f24", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f25", () -> myFaker.date().between(from, to, "yyyy-MM-dd'T'HH:mm:ss") ), 
		                Csv.Column.of("f26", () -> myFaker.date().between(from, to, "yyyy-MM-dd'T'HH:mm:ss") ),
		                Csv.Column.of("f27", () -> myFaker.MyElements().exponentialDistributedNumber(25, 99, 0) ),
		                Csv.Column.of("f28", () -> myFaker.MyElements().exponentialDistributedNumber(5, 19, 0) ),
		                Csv.Column.of("f29", () -> myFaker.MyElements().exponentialDistributedNumber(100, 200) ),
		                Csv.Column.of("f30", () -> myFaker.MyElements().nextDeterminedDistribElement("id30")),
		                Csv.Column.of("f31", () -> myFaker.expression("#{numerify '16704#####'}")) ,
		                Csv.Column.of("f32", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f33", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f34", () -> myFaker.expression("#{numerify '##'}")) ,	
		                Csv.Column.of("f35", () -> myFaker.expression("#{numerify '310004##########'}")) ,
		                Csv.Column.of("f36", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f37", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f38", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f39", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f40", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f41", () -> myFaker.MyElements().nextDeterminedDistribElement("id41")),
		                Csv.Column.of("f42", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f43", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f44", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f45", () -> myFaker.expression("#{numerify '#'}")) ,
		                Csv.Column.of("f45", () -> myFaker.expression("#{letterify 'A?'}").toUpperCase()) ,
		                Csv.Column.of("f45", () -> myFaker.expression("#{letterify 'B?'}").toUpperCase()) ,
		                Csv.Column.of("f45", () -> myFaker.expression("#{letterify 'C??'}").toUpperCase()) ,
		                Csv.Column.of("f49", () -> myFaker.expression("#{bothify 'D?##'}").toUpperCase()) ,
		                Csv.Column.of("f50", () -> myFaker.MyElements().exponentialDistributedNumber(20, 80, 2) ),
		                Csv.Column.of("f51", () -> myFaker.MyElements().exponentialDistributedNumber(20, 80, 2) ),
		                Csv.Column.of("f52", () -> myFaker.MyElements().exponentialDistributedNumber(20, 80, 2) ),
		                Csv.Column.of("f53", () -> myFaker.date().between(from, to, "yyyy-MM-dd'T'HH:mm:ss") ), 
		                Csv.Column.of("f54", () -> myFaker.MyElements().nextDeterminedDistribElement("id54"))
		                )
		            .header(true)
		            .separator(",")
		            .limit(10).build().get());

		
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
