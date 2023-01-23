package es.urj.doc_ti.jfcp.selec_reproc.gendata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Hashtable;

import net.datafaker.providers.base.AbstractProvider;
import net.datafaker.providers.base.BaseProviders;

public class MyElements extends AbstractProvider<BaseProviders> {
    private static final String[] INSECT_NAMES = new String[]{"Ant", "Beetle", "Butterfly", "Wasp"};
    
    private static String[] d1Names = {"N1", "N2", "N3"} ;
    private static int[] d1Vals = { 1, 1, 1 } ;
    
    static Hashtable<String, DistribData> distributions = new Hashtable<String, DistribData> () ;
    static {
    	loadDistrib( "id1", d1Names, d1Vals ) ;
     	DistribData data = new DistribData() ;
    	data.addIpsWithExponential(8, 1000, 1234);
    	distributions.put ( "ips" , data) ;    
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

	public String nextInsectName() {
        return INSECT_NAMES[faker.random().nextInt(INSECT_NAMES.length)];
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
