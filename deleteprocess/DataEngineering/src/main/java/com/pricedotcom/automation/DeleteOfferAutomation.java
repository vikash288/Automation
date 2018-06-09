package com.pricedotcom.automation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.util.EmailUtil;
import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

public class DeleteOfferAutomation {

	MysqlConnection mysqlconn = new MysqlConnection();
	MySqlOperation mysqlopp = new MySqlOperation();
	ProjectProperties prop = new ProjectProperties();
	MiscellaneousUtil util = new MiscellaneousUtil();
	EmailUtil mail = new EmailUtil();
	LogUtil logutil = new LogUtil();
	private final long splitLimit = 250000;
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	Logger logger = Logger.getLogger(DeleteOfferAutomation.class);

	public void offerDelete(String affiliator, String retailerName, String retailerNo) throws IOException {
		// Logger file initialization
		String logFileName = util.cleanSpecialCharacter(retailerName).toLowerCase() + "/"
				+ util.cleanSpecialCharacter(retailerName).toLowerCase() + "_" + df.format(new Date()) + "_delete.log";
		String targetTableName = prop.getDbtarget() + "." + util.cleanSpecialCharacter(affiliator) + "_"
				+ util.cleanSpecialCharacter(retailerName);

		try {
			logger.addAppender(logutil.getLogProp(logFileName));
			Connection conn = mysqlconn.connectMySQL(logFileName);

			String countQuery = "select count(*) from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC, MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN  "
					+ targetTableName
					+ " as TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = "
					+ Integer.parseInt(retailerNo)
					+ ") as a where not exists  (Select common_id, upc, retailer_code, item_id from PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc and a.retailer_code = "
					+ Integer.parseInt(retailerNo) + " and DATEDIFF(b.AUDITTIME, now())>=1)";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(countQuery);
			rs.next();
			long rowcnt = Long.parseLong(rs.getString(1));

			System.out.println("Row count "+rowcnt);
			
			//System.exit(0);
			
			//Row count more than 2.5 lakh
			if (rowcnt > splitLimit) {
				logger.info("Delete Process inside mod() query row count: "+rowcnt);
				logger.info("Delete Process Started");
				System.out.println("Delete Process inside mod() query row count: "+rowcnt);
				System.out.println("Delete Process Started");
				int splitTableCount = (int) (rowcnt / splitLimit);
				for (int i = 0; i < splitTableCount + 1; i++) {
					String splitSQL = "select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC, MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN  "
							+ targetTableName
							+ " as TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = "
							+ Integer.parseInt(retailerNo) + ") "
							+ " as a where not exists  (Select common_id, upc, retailer_code, item_id from PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc and a.retailer_code = "
							+ Integer.parseInt(retailerNo) + " and DATEDIFF(b.AUDITTIME, now())>=1) and mod(a.UPC,"
							+ (splitTableCount + 1) + ") = " + i;

					mysqlopp.deletePartialRecord(conn, targetTableName, retailerNo, retailerName, splitSQL,
							logFileName);
					

				}

				logger.info("Delete Process Completed");
				String mailBody = logutil.readLogFile(retailerName, ProjectProperties.getLogpath() + logFileName);
				mail.sendMailGet("Delete Offer Completed for " + affiliator + ":" + retailerName,
						"Hello, \n\n" + mailBody);

			} else {

				System.out.println("Delete Process Started row count: "+rowcnt);
				logger.info("Delete Process Started row count: "+rowcnt);
				String SQL = "select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC, MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN  "
						+ targetTableName
						+ " as TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = "
						+ Integer.parseInt(retailerNo)
						+ ") as a where not exists  (Select common_id, upc, retailer_code, item_id from PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc and a.retailer_code = "
						+ Integer.parseInt(retailerNo) + " and DATEDIFF(b.AUDITTIME, now())>=1)";
				mysqlopp.deletePartialRecord(conn, targetTableName, retailerNo, retailerName, SQL, logFileName);
				logger.info("Delete Process Completed");
				System.out.println("Delete Process Completed");
				String mailBody = logutil.readLogFile(retailerName, ProjectProperties.getLogpath() + logFileName);
				mail.sendMailGet("Delete Offer Completed for " + affiliator + ":" + retailerName,
						"Hello, \n\n" + mailBody);

			}
			
			conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) throws IOException {
		DeleteOfferAutomation offerdelete = new DeleteOfferAutomation();

		offerdelete.offerDelete("Linkshare", "LOFT", "185");
	}

}
