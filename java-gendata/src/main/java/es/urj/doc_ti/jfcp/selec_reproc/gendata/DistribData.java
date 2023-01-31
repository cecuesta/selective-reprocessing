package es.urj.doc_ti.jfcp.selec_reproc.gendata;

import java.util.ArrayList;
import java.util.Random;

import net.datafaker.Faker;
import net.datafaker.providers.base.BaseProviders;

public class DistribData {

	static final int MODE_IPV4 = 0 ;
	static final int MODE_CELL = 1 ;
	public static final int MODE_RANDOM_NAME = 2;
	public static final int MODE_RANDOM_GROUP = 3;
	
	
	int maxVal ;
	
	ArrayList<String>  arrN = new ArrayList<String>() ;
	ArrayList<Integer> arrV = new ArrayList<Integer>() ;

	
	public void add(String[] names, int[] vals) {
		maxVal = 0 ;
		
    	for ( int index = 0 ; index < names.length; index++) {
    		arrN.add(names[index]) ;
    		maxVal += vals[index] ;
    		arrV.add(maxVal) ;
    	}
	}
	
	public void addElementsWithExponential(int number, double lambda, int seed, int mode) {
		
		Random generator = new Random(seed);
		Faker fk = new Faker(generator) ;

		double mylamda = lambda<1 ? 1 : lambda ;
	    
    	for ( int index = 0 ; index < number; index++) {
    		
    		long num = 0 ;
    		while ( num <= 0 ) {
    			num = Math.round( -mylamda * Math.log(1-generator.nextDouble()) )  ;
    		}
    		
    		switch (mode) {
    		case MODE_IPV4: 
        		arrN.add(fk.internet().ipV4Address()) ;
    			break ;
    		case MODE_CELL: 
        		arrN.add(fk.expression("#{numerify '310004##########'}")) ;
        		break ;
    		case MODE_RANDOM_NAME: 
        		arrN.add(fk.funnyName().name().toLowerCase().replaceAll(" ", "_")) ;
        		break ;
    		case MODE_RANDOM_GROUP: 
        		arrN.add(fk.expression("#{bothify '????##??##'}")) ;
        		break ;
    		default :
        		arrN.add(fk.expression("#{bothify '?#?#?#?#'}").toUpperCase()) ;
        		break ;
    		}
    		
    		maxVal += num ;
    		arrV.add(maxVal) ;
    	}
    	
//    	System.out.println (arrV) ;
//    	System.out.println (arrN) ;
	}
	
	
	
	public String searchNext(BaseProviders faker) {
		
		int valMax = faker.random().nextInt(0, maxVal-1) ;
		
    	for ( int index = 0 ; index < arrN.size(); index++) {
    		if ( arrV.get(index) > valMax ) {
    			return arrN.get(index) ;
    		}
    	}
		
		return "N/F";
	}
	
	
}
