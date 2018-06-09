package com.pricedotcom.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.ProjectProperties;



public class MysqlConnection {
	
	LogUtil log = new LogUtil();
	Logger logger = Logger.getLogger(MysqlConnection.class);
	
	static ProjectProperties prop = new ProjectProperties();
	
	public Connection connectMySQL() throws ClassNotFoundException, SQLException, IOException {
		Class.forName(ProjectProperties.getDbclassname());
		Connection con = DriverManager.getConnection(ProjectProperties.getJdbcurl(),ProjectProperties.getDbusername(),ProjectProperties.getDbpassword());
		return con;
	}
	
	public Connection connectMySQL(String logpath) throws ClassNotFoundException, SQLException, IOException {
		logger.addAppender(log.getLogProp(logpath));
		logger.info("Mysql Connection Completed");
		Class.forName(ProjectProperties.getDbclassname());
		Connection con = DriverManager.getConnection(ProjectProperties.getJdbcurl(),ProjectProperties.getDbusername(),ProjectProperties.getDbpassword());
		return con;
	}
}
