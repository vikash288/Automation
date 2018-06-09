package com.pricedotcom.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

public class AutomationTest {

	public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException {
		Fileoperation fileOpp = new Fileoperation();
		MySqlOperation mysqlOpp = new MySqlOperation();
		MysqlConnection mysql = new MysqlConnection();
		Connection conn = mysql.connectMySQL();
		MarchentFTPConnection ftp = new MarchentFTPConnection();
		MiscellaneousUtil util = new MiscellaneousUtil();
		ProjectProperties prop = new ProjectProperties();

		String hostName = "aftp.linksynergy.com";
		String userName = "pricedotcom";
		String password = "JsGM93Yh";
		int port = 21;
		String sourceFolderName = "/";
		String sourceFile = "38733_3310515_mp.txt.gz";
		String retailer = "Sam's Club";
		String affiliator = "LinkShare";
		String retailerno = "25";

		List<String> stagingTableList = mysqlOpp.getAllTables(conn, "vikashsujoy");
		List<String> targetTableList = mysqlOpp.getAllTables(conn, "vikashsujoy");

		// String sfile = ftp.ftpDirectUpload(hostName, userName, password,
		// port, sourceFolderName, sourceFile, retailer);
		String sfile = "/home/abhinandan/DUMP/price/dump_1498645439131";
		long fileSize = fileOpp.getFileSize(sfile);
		System.out.println("File Size is : " + (double) fileSize / (1024 * 1024));

		String stagingTableName = "STG_" + util.cleanSpecialCharacter(affiliator) + "_"
				+ util.cleanSpecialCharacter(retailer) + "_W";
		String targetTableName = util.cleanSpecialCharacter(affiliator) + "_" + util.cleanSpecialCharacter(retailer);

		
		//Staging Table Creation and Load data
		
		/*if (stagingTableList.contains(stagingTableName)) {

			if ((double) fileSize / (1024 * 1024) > 10) {

				List<String> fileSplit = fileOpp.fileSplit(sfile);
				System.out.println("File Split Into :" + fileSplit.size() + " those Files are "
						+ Arrays.toString(fileSplit.toArray()));
				long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, "|", fileSplit);
				System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);

			} else {
				long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, "|", sfile);
				System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);

			}

		} else {

			mysqlOpp.createTable(conn, stagingTableName, affiliator);

			if ((double) fileSize / (1024 * 1024) > 10) {

				List<String> fileSplit = fileOpp.fileSplit(sfile);
				System.out.println("File Split Into :" + fileSplit.size() + " those Files are "
						+ Arrays.toString(fileSplit.toArray()));
				long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, "|", fileSplit);
				System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);

			} else {
				long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, "|", sfile);
				System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);

			}

		}*/

		// Target Table Creation
        long rowcnt = 0;
		if (targetTableList.contains(targetTableName)) {
			rowcnt=mysqlOpp.targetTableCreation(conn, affiliator, stagingTableName, targetTableName, retailerno, true);
		} else {
			rowcnt=mysqlOpp.targetTableCreation(conn, affiliator, stagingTableName, targetTableName, retailerno, false);
		}
		
		System.out.println(rowcnt);

		// fileOpp.deleteFile(sfile);
		conn.close();

	}

}
