package edu.doc_ti.jfcp.selec_reproc.interceptor.utils;

import java.io.File;

import org.apache.commons.io.FilenameUtils;

public class FileRegister {

	public FileRegister(){}
			
	public FileRegister(String file, int position) {
		super();
		this.file = file;
		this.position = position;
	}

	private String file = "";
	private int position = 0;

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}
	
	public String getFilename(String fullFileHeader){
		String fileNameWithExt = new File(fullFileHeader).getName();
		String fileNameWithOutExt = FilenameUtils.removeExtension(fileNameWithExt);
		return fileNameWithOutExt;
//		if (fileNameWithOutExt.matches(cdrDatosRegexF) ||
//				fileNameWithOutExt.matches(cdrVozRegexF)) {
//			return fileNameWithOutExt;
//		}
//			
//		return null;
	}
}
