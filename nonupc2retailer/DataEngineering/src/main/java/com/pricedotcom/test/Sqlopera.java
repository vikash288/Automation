package com.pricedotcom.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.mockito.internal.matchers.Find;


import com.pricedotcom.elasticsearch.ESOperations;
import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.pojo.DQMPrice;
import com.pricedotcom.util.LogUtil;

import au.com.bytecode.opencsv.CSVReader;


public class Sqlopera 
{

	private Fileoperation fileOpp = new Fileoperation();
	ClassLoader classLoader = getClass().getClassLoader();
	LogUtil log = new LogUtil();
	Logger logger = Logger.getLogger(Sqlopera.class);
	int i=0;
	
	public long loadStagingData(Connection conn, String affilator, String stagingTablename, String delimiter,
			String path, String logpath) {
		logger.addAppender(log.getLogProp(logpath));
		long rowcnt = 0;
		try {
		//	Statement stmt = conn.createStatement();
			
			ResultSet rs = null;
	//		String rowCount = "select count(*) from " + stagingTablename;
	//		String truncateTable = "TRUNCATE TABLE " + stagingTablename;
			String fileLoad = "";
			if (affilator.equalsIgnoreCase("linkshare")) {
				fileLoad = "load data local infile '" + "'?'"	 + "' into table " + "'?'"
						+ " fields terminated by '" + "'?'" + "' SET FIELD_39 = NOW();";
			} else if (affilator.equalsIgnoreCase("cj")) {
				fileLoad = "load data local infile '" + "'?'" + "' into table " + "'?'"
						+ " SET FIELD_41 = NOW();";
			} else if (affilator.equalsIgnoreCase("The RealReal")) {

				System.out.println("path is............." + path);

				fileLoad = "load data local infile '" + "'?'" + "' into table " + "'?'"
						+ " fields terminated by '" + "'?'" + "' SET FIELD_57 = NOW();";
			
			} else if (affilator.equalsIgnoreCase("realrealjson")) {
				fileLoad = "load data local infile '" + "'?'" + "' into table " + "'?'"
						+ " fields terminated by '" + "'?'" + "' SET DATE_TIME = NOW();";
			} else if (affilator.equalsIgnoreCase("snobswapjson")) {
				fileLoad = "load data local infile '" + "'?'" + "' into table " + "'?'"
						+ " fields terminated by '" + "'?'"
						+ "' ENCLOSED BY '\"' ESCAPED BY '' SET DATE_TIME = NOW();";
			} else {
				fileLoad = "load data local infile '" + "'?'" + "' into table " + "'?'"
						+ " fields terminated by '" + "'?'"
						+ "' ENCLOSED BY '\"' ESCAPED BY '' SET FIELD_24 = NOW();";
			}
			
			  PreparedStatement preparedStatement = conn.prepareStatement(fileLoad);
			  preparedStatement.setString(i++, path);
			  preparedStatement.setString(i++, stagingTablename);
			  preparedStatement.setString(i++, delimiter);
				System.out.println("file load is"+fileLoad);
			  preparedStatement.execute(fileLoad);
			  	

		//	stmt.execute(truncateTable);
		//	stmt.execute(fileLoad);
			logger.info("Staging table: " + stagingTablename + " truncated");
			logger.info("load data into Staging table: " + stagingTablename);
			fileOpp.deleteFile(path);
			
		//	rs=preparedStatement.executeQuery(rowCount);
			//rs = stmt.executeQuery(rowCount);
			rs.next();
	//		rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("SQL Exception " + e.toString());
		}
		return rowcnt;
	}

}
