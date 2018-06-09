package com.pricedotcom.test;

import java.io.File;

public class MainFolder {
	
	private void createDirectoryIfNeeded(String directoryName) {
		File theDir = new File(directoryName);
		System.out.println(theDir.exists());
		if (!theDir.exists())
			theDir.mkdirs();
	}
	
	
	public static void main(String args[]){
		MainFolder a = new MainFolder();
		a.createDirectoryIfNeeded("/home/abhinandan/Desktop/Bank/Sbi");
	}

}
