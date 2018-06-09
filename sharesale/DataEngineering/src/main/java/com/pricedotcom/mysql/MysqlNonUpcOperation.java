package com.pricedotcom.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.util.LogUtil;

public class MysqlNonUpcOperation {
	
	private Fileoperation fileOpp = new Fileoperation();
	ClassLoader classLoader = getClass().getClassLoader();
	LogUtil log = new LogUtil();
	Logger logger = Logger.getLogger(MysqlNonUpcOperation.class);
	
	
	public long loadNonUpcStagingData(Connection conn, String affilator, String stagingTablename, String delimiter,
			String path) {
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String truncateTable = "TRUNCATE TABLE " + stagingTablename;
			String fileLoad = "";
			if (affilator.equalsIgnoreCase("snobswap")) {
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " fields terminated by '" + delimiter + "' ENCLOSED BY '\"'  ESCAPED BY '' SET FIELD_24 = NOW();";
			}else if(affilator.equalsIgnoreCase("cj")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " SET FIELD_41 = NOW();";
			}

			stmt.execute(truncateTable);
			stmt.execute(fileLoad);

			fileOpp.deleteFile(path);

			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}
	
	
	
	public List<String> getCatInfo(Connection conn,String tableName,String title){
		List<String> result = new ArrayList<String>();
		try {
			String sql="select * from "+tableName+" where ";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if(rs.next()){
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}

}
