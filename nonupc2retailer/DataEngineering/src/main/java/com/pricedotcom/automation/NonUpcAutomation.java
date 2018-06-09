package com.pricedotcom.automation;

import java.io.File;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.pricedotcom.elasticsearch.NonUpcEsOperation;
import com.pricedotcom.fileoperation.FileOperationModifiedDataset;
import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.test.Sqlopera;
import com.pricedotcom.util.EmailUtil;
import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

/**
 * @author third-eye
 *
 */
public class NonUpcAutomation {

	private final Fileoperation fileOpp = new Fileoperation();
	private final MySqlOperation mysqlOpp = new MySqlOperation();
	private final MysqlConnection mysql = new MysqlConnection();
	private final MarchentFTPConnection ftp = new MarchentFTPConnection();
	private final ProjectProperties prop = new ProjectProperties();
	private final LogUtil logutil = new LogUtil();
	private final EmailUtil mail = new EmailUtil();
	private final MiscellaneousUtil util = new MiscellaneousUtil();
	private final NonUpcEsOperation esOperation = new NonUpcEsOperation();
	private final Sqlopera opp = new Sqlopera();

	private final FileOperationModifiedDataset dataset = new FileOperationModifiedDataset();

	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	// Add logger instance
	Logger logger = Logger.getLogger(NonUpcAutomation.class);
	private final static double splitFileSize = 256;
	private final static double splitTargetTableRow = 250000;

	@SuppressWarnings("static-access")
	public void nonUpcAutomate(String hostName, String userName, String password, int port, String sourceFolderName,
			String sourceFile, String retailer, String affiliator, String delimiter, String retailerno,
			String stagingTableName) {

		String sfile = "";
		long fileSize = 0;
		long rowcnt = 0;
		String dfile = "";
		long filecountjson = 0;

		List<String> files = new ArrayList<String>();

		boolean dqmstatus = false;
		boolean mailstatus = true;

		// Logger file initialization
		String logFileName = util.cleanSpecialCharacter(retailer).toLowerCase() + "/"
				+ util.cleanSpecialCharacter(retailer).toLowerCase() + "_" + df.format(new Date()) + ".log";

		logger.addAppender(logutil.getLogProp(logFileName));
		logger.info("Automation loading process started Retailer name: " + retailer + " Retailer No :" + retailerno
				+ " Affiliater name: " + affiliator);

		try {
			// MySQL Connection
			Connection conn = mysql.connectMySQL(logFileName);
			// String stagingTableName =
			// "PRICE_NOUPC_STG.STG_REALREAL_SHARESALE_DEMO_W";
			// String stagingTableOtherName =
			// "PRICE_NOUPC_STG.STG_SNOBSWAP_DEMO_W";
			String stagingTableNamejson = "PRICE_DOT_COM_STAGING.PRODUCT_CATEGORY";

			if (retailer.equalsIgnoreCase("The RealReal")) {
				sfile = ftp.ftpDirectUpload(hostName, userName, password, port, sourceFolderName, sourceFile, retailer,
						affiliator, logFileName);
				System.out.println("File Location :" + sfile);
				if (sfile.isEmpty()) {

					mailstatus = false;

					String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);
					mail.sendMailGet(affiliator + ":" + retailer.substring(0, 1).toUpperCase()
							+ retailer.substring(1).toLowerCase() + " File not Found in FTP ", mailBody);

				}

				else {

					files = dataset.readCSV(sfile, retailer, "\\|");

					fileSize = fileOpp.getFileSize(files.get(0));

					logger.info("File Size is : " + (double) fileSize / (1024 * 1024));

					// FTP File Download
					if ((double) fileSize / (1024 * 1024) > splitFileSize) {

						List<String> fileSplit = fileOpp.fileSplit(files.get(0));

						System.out.println("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));
						logger.info("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));

						long filecount = mysqlOpp.loadStagingData(conn, retailer, stagingTableName, delimiter,
								fileSplit, logFileName);
						System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);
						File fileCheck = new File(files.get(1));
						if (fileCheck.exists()) {
							filecountjson = mysqlOpp.loadStagingData(conn, "realrealjson", stagingTableNamejson,
									delimiter, files.get(1), logFileName);
						}

						System.out.println("Row Count in " + stagingTableName + " table is :" + filecountjson);

						logger.info("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);
						esOperation.getRows(stagingTableName, util.cleanSpecialCharacter(retailer).toLowerCase());

					} else {

						long filecount = mysqlOpp.loadStagingData(conn, retailer, stagingTableName, delimiter,
								files.get(0), logFileName);

						logger.info("Row Count in " + stagingTableName + " table is :" + filecount);

						File fileCheck = new File(files.get(1));
						if (fileCheck.exists()) {
							filecountjson = mysqlOpp.loadStagingData(conn, "realrealjson", stagingTableNamejson,
									delimiter, files.get(1), logFileName);
						}

						System.out.println("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);
						logger.info("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);
						logger.info("es operation started");
						esOperation.getRows(stagingTableName, util.cleanSpecialCharacter(retailer).toLowerCase());
						logger.info("es operation end");
					}
					
				}
			
				if(mailstatus)
					{
						String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);

						mail.sendMailGet(affiliator + ":" + retailer.substring(0, 1).toUpperCase()
								+ retailer.substring(1).toLowerCase() + ": " + " Espush operation Completed ", mailBody);
					}
			}
			// snobswap
			else {
				mailstatus=true;
				sfile = ftp.httpDownloadFile(hostName, retailer);
				System.out.println("sfile is......." + sfile);
				System.out.println("File Location :" + sfile);
				if (sfile.isEmpty()) {
					mailstatus = false;
					String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);

					mail.sendMailGet(affiliator + ":" + retailer.substring(0, 1).toUpperCase()
							+ retailer.substring(1).toLowerCase() + ": " + " File not Found in FTP ", mailBody);

				}

				else {

					files = dataset.read(sfile, retailer, ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
					fileSize = fileOpp.getFileSize(files.get(0));
					logger.info("File Size is : " + (double) fileSize / (1024 * 1024));
					System.out.println(files.get(0) + "........." + files.get(1));

					// FTP File Download
					if ((double) fileSize / (1024 * 1024) > splitFileSize) {

						List<String> fileSplit = fileOpp.fileSplit(files.get(0));
						System.out.println("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));
						logger.info("File Split Into :" + fileSplit.size() + " those Files are "
								+ Arrays.toString(fileSplit.toArray()));

						long filecount = mysqlOpp.loadStagingData(conn, retailer, stagingTableName, delimiter,
								fileSplit, logFileName);
						System.out.println("Row Count in " + stagingTableName + " table is :" + filecount);

						File fileCheck = new File(files.get(1));
						if (fileCheck.exists()) {
							filecountjson = mysqlOpp.loadStagingData(conn, "snobswapjson", stagingTableNamejson,
									delimiter, files.get(1), logFileName);
						}

						System.out.println("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);

						logger.info("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);
						logger.info("es operation started");
						esOperation.getRows(stagingTableName, util.cleanSpecialCharacter(retailer).toLowerCase());
						logger.info("es operation end");

					} else {
						long filecount = mysqlOpp.loadStagingData(conn, retailer, stagingTableName, delimiter,
								files.get(0), logFileName);
						logger.info("Row Count in " + stagingTableName + " table is :" + filecount);

						File fileCheck = new File(files.get(1));
						if (fileCheck.exists()) {

							filecountjson = mysqlOpp.loadStagingData(conn, "snobswapjson", stagingTableNamejson,
									delimiter, files.get(1), logFileName);
						}

						System.out.println("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);

						logger.info("Row Count in " + stagingTableNamejson + " table is :" + filecountjson);
						logger.info("es operation started");
						esOperation.getRows(stagingTableName, util.cleanSpecialCharacter(retailer).toLowerCase());
						logger.info("es operation end");
					}
				}
				if(mailstatus)
				{
					String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logFileName);

					mail.sendMailGet(affiliator + ":" + retailer.substring(0, 1).toUpperCase()
							+ retailer.substring(1).toLowerCase() + ": " + " Espush operation Completed ", mailBody);
				}
				
			}
			conn.close();			

		} catch (Exception e) {

			e.printStackTrace();
			logger.error("Exception Found : " + e.toString());
		}
	}

	public static void main(String args[]) {
		NonUpcAutomation automation = new NonUpcAutomation();

		/*
		 * String hostName = "aftp.linksynergy.com"; String userName =
		 * "pricedotcom"; String password = "JsGM93Yh"; int port = 21; String
		 * sourceFolderName = ""; String sourceFile =
		 * "42156_3310515_mp_delta.txt.gz"; String retailer = "Ann Taylor";
		 * String affiliator = "LinkShare"; String delimiter = "|"; String
		 * retailerno = "137";
		 */

		/*
		 * String hostName = "datatransfer.cj.com"; String userName = "4708603";
		 * String password = "oRvGWar%"; int port = 21; String sourceFolderName
		 * = "/outgoing/productcatalog/194608/"; String sourceFile =
		 * "PETCO_Animal_Supplies-Product_Catalog.txt.gz"; String retailer =
		 * "Petco"; String affiliator = "cj"; String delimiter = " "; String
		 * retailerno = "6";
		 */

		/*
		 * String hostName = "datafeeds.shareasale.com"; String userName =
		 * "pricedotcom"; String password = "Price$2017$"; int port = 21; String
		 * sourceFolderName = "/32926"; String sourceFile = "32926.zip"; String
		 * retailer = "The RealReal"; String affiliator = "Shareasale"; String
		 * delimiter = "|"; String retailerno = "203";
		 */

		/*
		 * String hostName = "aftp.linksynergy.com"; String userName =
		 * "pricedotcom"; String password = "JsGM93Yh"; int port = 21; String
		 * sourceFolderName = ""; String sourceFile = "38464_3310515_mp.txt.gz";
		 * String retailer = "New York & Company"; String affiliator =
		 * "Linkshare"; String delimiter = "|"; String retailerno = "210";
		 */

		/*
		 * String hostName = "https://snobswap.com/googleshopping.csv"; String
		 * userName = ""; String password = ""; int port = 0; String
		 * sourceFolderName = ""; String sourceFile = ""; String retailer = "";
		 * String affiliator = ""; String delimiter = ","; String retailerno
		 * ="";
		 */

		/*
		 * automation.nonUpcAutomate(hostName, userName, password, port,
		 * sourceFolderName, sourceFile, retailer, affiliator, delimiter,
		 * retailerno,"PRICE_NOUPC_STG.STG_REALREAL_SHARESALE_DEMO_W");
		 */

	}

}
