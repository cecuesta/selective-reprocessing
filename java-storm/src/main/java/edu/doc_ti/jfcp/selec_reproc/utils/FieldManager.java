package edu.doc_ti.jfcp.selec_reproc.utils;

import java.util.HashMap;

public class FieldManager {

	public static HashMap<Integer, String> fieldNames = new HashMap<Integer, String>() ;

	static {
		for (int index = 0 ; index < 100 ; index++) {
			fieldNames.put(index, "field_" + index ) ;
			fieldNames.put(0, "filename") ;
			fieldNames.put(1, "line_number") ;
			fieldNames.put(24, "date_end") ;
		}
		
	}
	
}
