package com.pricedotcom.automation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.pricedotcom.elasticsearch.ESOperations;
import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.pojo.DQMPrice;
import com.pricedotcom.util.EmailUtil;
import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

public class Automation {

	private final Fileoperation fileOpp = new Fileoperation();
	private final MySqlOperation mysqlOpp = new MySqlOperation();
	private final MysqlConnection mysql = new MysqlConnection();
	private final MarchentFTPConnection ftp = new MarchentFTPConnection();
	private final MiscellaneousUtil util = new MiscellaneousUtil();
	private final ProjectProperties prop = new ProjectProperties();
	private final LogUtil logutil = new LogUtil();
	private final EmailUtil mail = new EmailUtil();
	private final ESOperations esopp = new ESOperations();

	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	// Add logger instance
	Logger logger = Logger.getLogger(Automation.class);
	private final static double splitFileSize = 256;
	private final static double splitTargetTableRow = 250000;

	@SuppressWarnings("static-access")
	public void automate(String hostName, String userName, String password, int port, String sourceFolderName,
			String sourceFile, String retailer, String affiliator, String delimiter, String retailerno, String loadtype,
			int s3flag, int mysqlflag, int esflag) {

		// Initialized Variable
		String sfile = "";
		long fileSize = 0;
		long rowcnt = 0;

		boolean dqmstatus = false;
		boolean mailstatus = true;

		// Logger file initialization
		String logFileName = util.cleanSpecialCharacter(retailer).toLowerCase() + "/"
				+ util.cleanSpecialCharacter(retailer).toLowerCase() + "_" + df.format(new Date()) + ".log";

		logger.addAppender(logutil.getLogProp(logFileName));
		logger.info("Automation loading process started Retailer name: " + retailer + " Retailer No :" + retailerno
				+ " Affiliater name: " + affiliator);
		try {

			// FTP Connection and S3 Uplaod block
			if (s3flag == 1) {

				if (affiliator.equalsIgnoreCase("cj") && (loadtype.equalsIgnoreCase("incremental") || loadtype.equalsIgnoreCase("full"))) 
				{ 
					// Date Difference in CJ
					int dateDiff = ftp.ftpFileModificationDate(hostName, userName, password, port, sourceFolderName,
							sourceFile, retailerno, logFileName);
					
					System.out.println("Date diff is "+dateDiff);

					if (dateDiff <= 1) {
						// FTP File Download
						sfile = ftp.ftpDirectUpload(hostName, userName, password, port, sourceFolderName, sourceFile,
								retailer,affiliator, logFileName);
						System.out.println("File Location :" + sfile);

						if (sfile.isEmpty()) {
							mailstatus = false;

							String mailBody = logutil.readLogFile(retailer,
									ProjectProperties.getLogpath() + logFileName);
							/*mail.sendMailGet(affiliator + ":" + retailer.substring(0, 1).toUpperCase()
									+ retailer.substring(1).toLowerCase() + ": " + loadtype + " File not Found in FTP ",
									mailBody);*/

						}

						fileSize = fileOpp.getFileSize(sfile);
						System.out.println("File Size is : " + (double) fileSize / (1024 * 1024));
						logger.info("File Size is : " + (double) fileSize / (1024 * 1024));
					} else {
						logger.warn(
								"No data will incremental load , because based on ftp file date differnce is "+dateDiff);

						mailstatus = false;

						String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);
					/*	mail.sendMailGet(
								affiliator + ":" + retailer.substring(0, 1).toUpperCase()
										+ retailer.substring(1).toLowerCase() + ": " + loadtype
										+ " has no Incremental file for Today  ",
								"Hello, \n Today There is no Incremental load for retailer: " + retailer
										+ " \n\n You can see the log " + mailBody);*/

					}

				} else {

					// FTP File Download
					sfile = ftp.ftpDirectUpload(hostName, userName, password, port, sourceFolderName, sourceFile,
							retailer,affiliator ,logFileName);
					System.out.println("File Location :" + sfile);

					if (sfile.isEmpty()) {
						mailstatus = false;

						String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);
						/*mail.sendMailGet(affiliator + ":" + retailer.substring(0, 1).toUpperCase()
								+ retailer.substring(1).toLowerCase() + ": " + loadtype + " File not Found in FTP ",
								mailBody);*/
					}

					fileSize = fileOpp.getFileSize(sfile);
					System.out.println("File Size is : " + (double) fileSize / (1024 * 1024));
					logger.info("File Size is : " + (double) fileSize / (1024 * 1024));
				}
			}

			
			
			
			// MySQL Connection
			Connection conn = mysql.connectMySQL(logFileName);

			List<String> stagingTableList = mysqlOpp.getAllTables(conn, prop.getDbstg());
			List<String> targetTableList = mysqlOpp.getAllTables(conn, prop.getDbtarget());

			String stagingTableName = prop.getDbstg() + ".STG_" + util.cleanSpecialCharacter(affiliator) + "_"
					+ util.cleanSpecialCharacter(retailer) + "_W";
			String targetTableName = prop.getDbtarget() + "." + util.cleanSpecialCharacter(affiliator) + "_"
					+ util.cleanSpecialCharacter(retailer);
			
			
			
			

			// S3 to Mysql Load
			/*if (mysqlflag == 1 && !sfile.isEmpty()) {
				//////////// Staging Table Creation and Load data///////////////

				if (stagingTableList.contains(stagingTableName)) {

					logger.info("Staging table found Staging Table name: " + stagingTableName);

					if ((double) fileSize / (1024 * 1024) > splitFileSize) {

						List<String> fileSplit = fileOpp.fileSplit(sfile);
						System.out.println("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));
						logger.info("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));

						long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, delimiter,
								fileSplit, logFileName);
						System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);

						logger.info("Row Count in " + stagingTableName + " table is :" + filecount);

					} else {
						long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, delimiter, sfile,
								logFileName);
						System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);
						logger.info("Row Count in " + stagingTableName + " table is :" + filecount);

					}

				} else {

					mysqlOpp.createTable(conn, stagingTableName, affiliator);
					logger.info("New Staging Table Created , Table Name: " + stagingTableName);

					if ((double) fileSize / (1024 * 1024) > splitFileSize) {

						List<String> fileSplit = fileOpp.fileSplit(sfile);
						System.out.println("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));

						logger.info("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));

						long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, delimiter,
								fileSplit, logFileName);
						System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);
						logger.info("Row Count in " + stagingTableName + " table is :" + filecount);

					} else {
						long filecount = mysqlOpp.loadStagingData(conn, affiliator, stagingTableName, delimiter, sfile,
								logFileName);
						System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);
						logger.info("Row Count in " + stagingTableName + " table is :" + filecount);

					}

				}

				// Target Table Creation

				if (targetTableList.contains(targetTableName)) {
					logger.info("Data Insert process start in Traget table: " + targetTableName);
					rowcnt = mysqlOpp.targetTableCreation(conn, affiliator, stagingTableName, targetTableName,
							retailerno, true, logFileName);
				} else {
					logger.info(
							"New Target Table created data insert process start in Traget table: " + targetTableName);
					rowcnt = mysqlOpp.targetTableCreation(conn, affiliator, stagingTableName, targetTableName,
							retailerno, false, logFileName);
				}

				System.out.println(rowcnt);
				logger.info("Total row count in Traget table: " + targetTableName + " is " + rowcnt);

				// DQM Module to check price
				List<DQMPrice> price = mysqlOpp.dqmPriceCheck(conn, targetTableName);
				if (price.size() > 0) {
					double price1 = price.get(0).getPrice1();
					double price2 = price.get(0).getPrice2();

					if (price1 == 0 && price2 == 0) {

						dqmstatus = false;
						
						if (mailstatus) {
							logger.warn("Price 1 and Price2 both have no suitable field");
							String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);
							mail.sendMailGet(
									"Data Consistency Issue : Affiliate: " + affiliator + "Retailer: " + retailer
											+ "Price1,Price2",
									"Hello, \n " + targetTableName + ".Price1 and " + targetTableName
											+ ".Price2 fields have missing values. \n Please take suitable actions before Support Team pushes to ES. \n\n Logs :\n\n"+mailBody+" \n Thank you, \n Price.com Data Engineering Support Team");
							
						}
						
						mailstatus = false;


					} else if (price1 == 0 && price2 > 0) {

						dqmstatus = true;
						mysqlOpp.updatepriceDQM(conn, targetTableName, "price1");
						logger.warn("DQM module initiated to Check and correct Price1 value");

					} else if (price1 > 0 && price2 == 0) {

						dqmstatus = true;
						mysqlOpp.updatepriceDQM(conn, targetTableName, "price2");
						logger.warn("DQM module initiated to Check and correct Price1 value");

					} else {

						dqmstatus = true;

					}
				}
				
				//Delete Partial Record on Full load
				if(loadtype.equalsIgnoreCase("full")){
				  mysqlOpp.deletePartialRecord(conn, targetTableName, retailerno, util.cleanSpecialCharacter(retailer).toLowerCase(), logFileName);
				}
			}

			fileOpp.deleteFile(sfile);
			conn.close();

			if (dqmstatus && rowcnt > 0) {
				// Full Target table ES PUSH
				logger.info("ES Push Started");
				if (rowcnt > splitTargetTableRow) {
					// Split the target table for Enhance performance on ES Push
					int splitTableCount = (int) (rowcnt / splitTargetTableRow);
					for (int i = 0; i < splitTableCount + 1; i++) {
						String SplitSQL = "select * from " + targetTableName + " where mod(upc," + (splitTableCount + 1)
								+ ")=" + i;
						esopp.getQuerys(SplitSQL, util.cleanSpecialCharacter(retailer).toLowerCase());
					}

					logger.info("ES Push Completed");

				} else {

					// Full Target table ES PUSH
					logger.info("ES Push Started");
					esopp.getRows(targetTableName, util.cleanSpecialCharacter(retailer).toLowerCase());
					logger.info("ES Push Completed");

				}

				if (mailstatus) {
					// Mail Module initiated when no issue in s3 load, cj
					// incremental load and dqm module
					String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);
					mail.sendMailGet(
							affiliator + ":" + retailer.substring(0, 1).toUpperCase()
									+ retailer.substring(1).toLowerCase() + ":" + loadtype + " Load-Completed",
							mailBody);
					
				}

			} */

		} catch (ClassNotFoundException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Exception Found : " + e.toString());
		}

	}

	public static void main(String args[]) {
		Automation auto = new Automation();

		/*String hostName = "aftp.linksynergy.com";
		String userName = "pricedotcom";
		String password = "JsGM93Yh";
		int port = 21;
		String sourceFolderName = "";
		String sourceFile = "42156_3310515_mp_delta.txt.gz";
		String retailer = "Ann Taylor";
		String affiliator = "LinkShare";
		String delimiter = "|";
		String retailerno = "137";
*/
		
		 /* String hostName = "datatransfer.cj.com";
		  String userName = "4708603";
		  String password = "oRvGWar%"; 
		  int port = 21; 
		  String sourceFolderName = "/outgoing/productcatalog/194608/"; 
		  String sourceFile = "PETCO_Animal_Supplies-Product_Catalog.txt.gz"; 
		  String retailer = "Petco"; 
		  String affiliator = "cj"; 
		  String delimiter = " "; String
		  retailerno = "6";*/
		
		
		String hostName = "datafeeds.shareasale.com";
		String userName = "pricedotcom";
		String password = "Price$2017$";
		int port = 21;
		String sourceFolderName = "/32926";
		String sourceFile = "32926.zip";
		String retailer = "The RealReal";
		String affiliator = "Shareasale";
		String delimiter = "|";
		String retailerno = "203";
		
		 

		auto.automate(hostName, userName, password, port, sourceFolderName, sourceFile, retailer, affiliator, delimiter,
				retailerno, "incremental", 1, 1, 1);

	}

}
