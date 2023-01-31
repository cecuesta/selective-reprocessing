package es.urj.doc_ti.jfcp.selec_reproc.gendata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Hashtable;

import net.datafaker.providers.base.AbstractProvider;
import net.datafaker.providers.base.BaseProviders;

public class MyElements extends AbstractProvider<BaseProviders> {
    
    private static String[] d1Names = {"N1", "N2", "N3"} ;
    private static int[] d1Vals = { 1, 1, 1 } ;
    
    private static int BASE_SEED = 1234 ;
    
    static Hashtable<String, DistribData> distributions = new Hashtable<String, DistribData> () ;
    static {
    	loadDistrib( "id1", d1Names, d1Vals ) ;

    	final String[] id3N = {"18", "19"};
        final int[]    id3V = { 1, 1 } ;
    	loadDistrib( "id3", id3N, id3V ) ;
    	
    	DistribData data = new DistribData() ;
    	data.addElementsWithExponential(500, 1000, BASE_SEED, DistribData.MODE_CELL);
    	distributions.put ( "id4" , data) ;
    	
    	data= new DistribData() ;
    	data.addElementsWithExponential(6, 1000000, BASE_SEED, DistribData.MODE_IPV4);
    	distributions.put ( "id5" , data) ;

    	data = new DistribData() ;
    	data.addElementsWithExponential(2, 1000, BASE_SEED, DistribData.MODE_IPV4);
    	distributions.put ( "id7" , data) ;

    	data = new DistribData() ;
    	data.addElementsWithExponential(8, 1000, BASE_SEED, DistribData.MODE_RANDOM_NAME);
    	distributions.put ( "id8" , data) ;

    	final String[] id9N = {"12345", "12346"};
        final int[]    id9V = { 4035032, 34390 } ;
    	loadDistrib( "id9", id9N, id9V ) ;
    	
    	data= new DistribData() ;
    	data.addElementsWithExponential(1000, 10000, BASE_SEED, DistribData.MODE_IPV4);
    	distributions.put ( "id10" , data) ;    
    	
    	data = new DistribData() ;
    	data.addElementsWithExponential(400, 10000, BASE_SEED, DistribData.MODE_RANDOM_GROUP);
    	distributions.put ( "id30" , data) ;

    	final String[] id41N = {"01", "02", "03", "04", "05", "06"};
        final int[]    id41V = { 3169134 , 659245 , 167812, 27169, 7170, 4317} ;
    	loadDistrib( "id41", id41N, id41V ) ;
    	
    	
    	final String[] id54N = {"2G", "3G", "4G", "5G"};
        final int[]    id54V = { 1000 , 5000, 50000, 15000} ;
    	loadDistrib( "id54", id54N, id54V ) ;
   }
    

    public MyElements(BaseProviders faker)  {
        super(faker);
    }
    
    
    private static void loadDistrib(String name, String[] names, int[] vals) {

     	DistribData data = new DistribData() ;
    	data.add(names, vals) ;
    	distributions.put ( name , data) ;
	}

    public String nextDeterminedDistribElement( String name) {
    	DistribData d = distributions.get(name) ;
    	if ( d == null ) {
    		return "N/A";
    	}
    	
        return d.searchNext(faker) ;
    } 

    public String exponentialDistributedNumber(double lambda) {
    	return exponentialDistributedNumber(lambda,  Long.MAX_VALUE ) ;
    }

    public String exponentialDistributedNumber(double lambda, long maxValue ) {
    	return exponentialDistributedNumber(lambda,  maxValue, 3 ) ;
    }
    
    public String exponentialDistributedNumber(double lambda, long maxValue, int maxNumberOfDecimals ) {
    	
    	final BigDecimal max = BigDecimal.valueOf(maxValue) ;
    	
    	while ( true ) { 
	    	final BigDecimal random = BigDecimal.valueOf( -lambda * Math.log(1-faker.random().nextDouble())  );
	     
	    	if ( random.compareTo(max ) < 0 ) {
		    	return random.setScale(maxNumberOfDecimals, RoundingMode.HALF_DOWN)
		                .toString();
	    	}
    	}
    }  

    public double exponentialDistributedNumberAsDouble(double lambda) {
    	return -lambda * Math.log(1-faker.random().nextDouble())   ;
    }
    
    
    

}
