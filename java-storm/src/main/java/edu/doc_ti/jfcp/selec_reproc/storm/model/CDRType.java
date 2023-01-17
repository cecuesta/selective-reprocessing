package edu.doc_ti.jfcp.selec_reproc.storm.model;

import java.util.List;

public class CDRType {

	private Integer positionData;
	private List<Integer> positions;
	private String type;
	private String flow;
	
	public CDRType(Integer positionData, List<Integer> positions, String type, String flow) {
		super();
		this.positionData = positionData;
		this.positions = positions;
		this.type = type;
		this.setFlow(flow);
	}
	
	public Integer getPositionData() {
		return positionData;
	}
	
	public void setPositionData(Integer positionData) {
		this.positionData = positionData;
	}
	
	public List<Integer> getPositions() {
		return positions;
	}
	
	public void setPositions(List<Integer> positions) {
		this.positions = positions;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFlow() {
		return flow;
	}

	public void setFlow(String flow) {
		this.flow = flow;
	}
}
