package edu.doc_ti.jfcp.selec_reproc.storm.bolt;


public class BoltBuilder {

	public ProcessBolt buildProcessBolt(){
		return new ProcessBolt();
	}
	
	public ESInserterBolt buildESInserterBolt() {
		return new ESInserterBolt();
	}


}
