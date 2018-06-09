package com.pricedotcom.nonupc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.pricedotcom.elasticsearch.ESOperations;
import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.restapi.PriceRestAPI;
import com.pricedotcom.util.EmailUtil;
import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

public class SnobswapAutomation {
	
	private final Fileoperation fileOpp = new Fileoperation();
	private final MySqlOperation mysqlOpp = new MySqlOperation();
	private final MysqlConnection mysql = new MysqlConnection();
	private final MarchentFTPConnection ftp = new MarchentFTPConnection();
	private final MiscellaneousUtil util = new MiscellaneousUtil();
	private final ProjectProperties prop = new ProjectProperties();
	private final LogUtil logutil = new LogUtil();
	private final EmailUtil mail = new EmailUtil();
	private final ESOperations esopp = new ESOperations();
	private final PriceRestAPI restapi = new PriceRestAPI();

	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	// Add logger instance
		Logger logger = Logger.getLogger(SnobswapAutomation.class);
		private final static double splitFileSize = 256;
		private final static double splitTargetTableRow = 250000;
	
	public void snobswapAutomation(String url, String retailer, String affiliator, String delimiter, String retailerno, String loadtype,
			int s3flag, int mysqlflag, int esflag){
		
		String sfile = "";
		long fileSize = 0;
		long rowcnt = 0;
		
		//create dump folder if not exits
	    try {
			fileOpp.createDirectoryIfNeeded(prop.getDumpfilepath());
			
			// Logger file initialization
			String logFileName = util.cleanSpecialCharacter(retailer).toLowerCase() + "/"
					+ util.cleanSpecialCharacter(retailer).toLowerCase() + "_" + df.format(new Date()) + ".log";

			logger.addAppender(logutil.getLogProp(logFileName));
			logger.info("Automation loading process started Retailer name: " + retailer + " Retailer No :" + retailerno
					+ " Affiliater name: " + affiliator);
			
			if (s3flag == 1) {
				sfile = ftp.httpDownloadFileS3(url, retailerno);
				fileSize = fileOpp.getFileSize(sfile);
				
			}
			
			// MySQL Connection
			Connection conn = mysql.connectMySQL(logFileName);
			
			// Write file with cat id
			if (!sfile.isEmpty()) {
				String line = "";
				String updatedFile = prop.getDumpfilepath() + "/updatedgoogleshopping.csv";
				PrintWriter writer = new PrintWriter(updatedFile, "UTF-8");
				BufferedReader br = new BufferedReader(new FileReader(sfile));
				br.readLine();
				while ((line = br.readLine()) != null) {
					
					// use comma as separator
					String[] lines = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
					System.out.println("old line: " + lines.length);
					String jsonData = restapi.getCatId(lines[1].replace("\"", ""));
					
					JSONObject obj = new JSONObject(jsonData);

					String parent_cat_id = obj.get("parent_cat_id").toString();
					String parent_name = obj.get("parent_name").toString();
					String cat_id = obj.get("cat_id").toString();
					String crumb = obj.get("crumb").toString();
					String name = obj.get("name").toString();

					String newLine = line + "," + "\"" + parent_cat_id + "\"," + "\"" + parent_name + "\"," + "\""
							+ cat_id + "\"," + "\"" + crumb + "\"," +"\""+ name + "\"";
					
					writer.println(newLine);
					
				}
			}

			
			
			
			
			
			
			
			
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}

}
