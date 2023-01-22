package es.urj.doc_ti.jfcp.selec_reproc.gendata;

import java.util.ArrayList;

import net.datafaker.Faker;
import net.datafaker.providers.base.BaseProviders;

public class DistribData {

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
