package com.pricedotcom.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.PatternLayout;

public class LogUtil {

	/**
	 * @param fileName
	 * @return
	 */
	public FileAppender getLogProp(String fileName) {

		// creates file appender
		FileAppender fileAppender = new FileAppender();
		try {
			// creates pattern layout
			PatternLayout layout = new PatternLayout();
			String conversionPattern = "%-5p %d{yyyy-MM-dd HH:mm:ss} %c:%L %x - %m%n";
			// %-7p %d [%t] %c %x - %m%n
			layout.setConversionPattern(conversionPattern);

			/*
			 * // creates console appender ConsoleAppender consoleAppender = new
			 * ConsoleAppender(); consoleAppender.setLayout(layout);
			 * consoleAppender.activateOptions();
			 */

			fileAppender.setFile(ProjectProperties.getLogpath() + fileName);

			fileAppender.setLayout(layout);
			fileAppender.activateOptions();

			// configures the root logger

			/*Logger rootLogger = Logger.getRootLogger();
			rootLogger.setLevel(Level.DEBUG);
			// rootLogger.addAppender(consoleAppender);
			rootLogger.addAppender(fileAppender);*/

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileAppender;
	}
	
	
	/**
	 * @param string2 
	 * @param logPath
	 * @return
	 */
	public String readLogFile(String retailerName,String logPath){
		String content="Data Process for " +retailerName+"\n";
		try (BufferedReader br = new BufferedReader(new FileReader(logPath))) {

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				//System.out.println(sCurrentLine);
				content+=sCurrentLine+"\n";
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
		
	}

	
	
	public static void main(String args[]){
		LogUtil log = new LogUtil();
		System.out.println(log.readLogFile("petco","/home/abhinandan/DUMP/log/petco/petco_2017-06-30.log"));
	}
}
