package edu.doc_ti.jfcp.selec_reproc.storm.model;

import java.util.List;

public class CDRField {
	private String name;
	private List<String> calculatedFields;
	private String type;
	
	public CDRField (String name, List<String> calculatedFields,String type){
		this.setName(name);
		this.setCalculatedFields(calculatedFields);
		this.setType(type);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getCalculatedFields() {
		return calculatedFields;
	}

	public void setCalculatedFields(List<String> calculatedFields) {
		this.calculatedFields = calculatedFields;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
