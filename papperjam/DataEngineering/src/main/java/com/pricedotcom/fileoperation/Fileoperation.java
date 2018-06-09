package com.pricedotcom.fileoperation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;



public class Fileoperation {
	
	Logger logger = Logger.getLogger(Fileoperation.class);

	/**
	 * @param path
	 * @return
	 */
	public long getFileSize(String path) {
		long kilobytes = 0;
		File file = new File(path);
		if (file.exists()) {
			kilobytes = file.length();
		}
		return kilobytes;
	}

	/**
	 * @param path
	 * @return
	 */
	public List<String> fileSplit(String path) {
		int partCounter = 1;// I like to name parts from 001, 002, 003, ...
		// you can change it to 0 if you want 000, 001, ...
		List<String> fileList = new ArrayList<String>();
		int sizeOfFiles = 1024 * 1024 * 250;// 250MB
		byte[] buffer = new byte[sizeOfFiles];
		File file = new File(path);
		if (file.exists()) {
			String fileName = file.getName();

			// try-with-resources to ensure closing stream
			try {
				FileInputStream fis = new FileInputStream(file);
				BufferedInputStream bis = new BufferedInputStream(fis);
				int bytesAmount = 0;
				while ((bytesAmount = bis.read(buffer)) > 0) {
					// write each chunk of data into separate file with
					// different number in name
					String filePartName = String.format("%s.%03d", fileName, partCounter++);
					File newFile = new File(file.getParent(), filePartName);
					fileList.add(newFile.getAbsolutePath());
					try (FileOutputStream out = new FileOutputStream(newFile)) {
						out.write(buffer, 0, bytesAmount);
					}
				}
				fis.close();
				bis.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return fileList;

	}

	/**
	 * @param path
	 * @return
	 */
	public boolean deleteFile(String path) {
		File file = new File(path);
		if (file.delete()) {
			return true;
		} else {
			return false;
		}

	}
	
	
	/**
	 * @param directoryName
	 */
	public void createDirectoryIfNeeded(String directoryName) {
		File theDir = new File(directoryName);
		if (!theDir.exists())
			theDir.mkdirs();
	}
	
	
	public void readFiles(String filePath) {

		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream(filePath);
			sc = new Scanner(inputStream, "UTF-8");
			int i =0;
			while (sc.hasNextLine()) {
				i++;
				String line = sc.nextLine();
				line.replace("\"", "");
				String title[] = line.replace("\"", "").split(",");
				System.out.println(title[1]);
				if(i==5){
				System.exit(0);
				}
			}
			// note that Scanner suppresses exceptions
			if (sc.ioException() != null) {
				throw sc.ioException();
			}
		} catch (Exception e) {

			e.printStackTrace();

		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sc != null) {
				sc.close();
			}
		}

	}
	
	
	public static void main(String args[]){
		Fileoperation fileopp = new Fileoperation();
		fileopp.readFiles("/home/abhinandan/Projects/Price.com/dump/googleshopping.csv");
	}

}
