package com.pricedotcom.DataEngineering;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.pricedotcom.automation.Automation;
import com.pricedotcom.automation.DeleteOfferAutomation;
import com.pricedotcom.automation.NonUpcAutomation;
import com.pricedotcom.elasticsearch.ESOperations;
import com.pricedotcom.elasticsearch.ElasticSearchClient;
import com.pricedotcom.kafka.MarchentDataConsumer;
import com.pricedotcom.kafka.MarchentDataIngestion;
import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.test.CleanProductURL;
import com.pricedotcom.test.ElasticSearchClientTest;

public class TransferFiles {

	// final static Logger logger = Logger.getLogger(TransferFiles.class);
	final static MarchentDataConsumer consumer = new MarchentDataConsumer();
	final static MarchentDataIngestion producer = new MarchentDataIngestion();
	final static ElasticSearchClient es = new ElasticSearchClient();
	final static MarchentFTPConnection ftp = new MarchentFTPConnection();
	final static CleanProductURL cleanurl = new CleanProductURL();
	final static ElasticSearchClientTest testes = new ElasticSearchClientTest();
	final static Automation automation = new Automation();
	final static ESOperations newesopp = new ESOperations();
	final static DeleteOfferAutomation deleteOffer = new DeleteOfferAutomation();
	final static NonUpcAutomation nonupcautomation=new NonUpcAutomation();

	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
		// logger.info("File Transfer Process Start");
		System.out.println("File Transfer Process Start");
		String classtype = args[0];

		try {
			System.out.println(classtype);
			if (classtype.equals("producer")) {
				String ftpHostname = args[1];
				String ftpUsername = args[2];
				String ftpPassword = args[3];
				int port = Integer.parseInt(args[4]);
				String sourceName = args[5];
				String sourceFolderName = args[6];
				String sourceFile = args[7];
				String topic = args[8];
				String retailer = args[9];
				producer.dataIngestion(ftpHostname, ftpUsername, ftpPassword, port, sourceFolderName, sourceFile, topic,
						retailer);
			} else if (classtype.equals("consumer")) {
				String topic = args[1];
				consumer.dataConsumer(topic);
			} else if (classtype.equals("table")) {
				String tablename = args[1];
				String retailerName = args[2];
				es.getRows(tablename, retailerName);
			} else if (classtype.equals("query")) {
				String query = args[1];
				es.getRowsQuery(query);
			} else if (classtype.equals("s3load")) {
				String ftpHostname = args[1];
				String ftpUsername = args[2];
				String ftpPassword = args[3];
				int port = Integer.parseInt(args[4]);
				String sourceName = args[5];
				String sourceFolderName = args[6];
				String sourceFile = args[7];
				String topic = args[8];
				String retailer = args[9];
				ftp.ftpDirectUpload(ftpHostname, ftpUsername, ftpPassword, port, sourceFolderName, sourceFile, retailer);
			} else if(classtype.equals("directload")){
				String sourceFile = args[1];
				String topic = args[2];
				String retailer = args[3];
				ftp.httpDownloadFile(sourceFile, retailer);
			}else if(classtype.equals("cleantable")){
				String tablename = args[1];
				cleanurl.cleanTable(tablename);
			}else if(classtype.equals("amazon")){
				//testes.getRows("", "amazon");
			}
			else if(classtype.equals("ebay-multi")) {
				String groupbyTableName = args[1];
				String tableName = args[2];
				String retailerName = args[3];
				testes.getRowsMulti(groupbyTableName, tableName, retailerName);
			}
			else if(classtype.equals("ebay")) {
				String tableName = args[1];
				String retailerName = args[2];
				testes.getRows(tableName, retailerName);
			}else if(classtype.equals("automation")){
				
				String hostName = args[1];
				String userName = args[2];
				String password = args[3];
				int port = Integer.parseInt(args[4]);
				String sourceFolderName =args[5];
				String sourceFile = args[6];
				String retailer = args[7];
				String affiliator = args[8];
				String retailerno = args[10];
				String delimiter = args[9];
				String loadType = args[11];
				int s3flag = Integer.parseInt(args[12]);
				int mysqlflag = Integer.parseInt(args[13]);
				int esflag = Integer.parseInt(args[14]);
				
				automation.automate(hostName, userName, password, port, sourceFolderName, sourceFile, retailer, affiliator, delimiter, retailerno,loadType,s3flag,mysqlflag,esflag);
				
			}else if(classtype.equals("autoespush")){
				String tablename = args[1];
				String retailerName = args[2];
				newesopp.getRows(tablename, retailerName);
			}else if(classtype.equals("deleteautomation")){
				String hostName = args[1];
				String userName = args[2];
				String password = args[3];
				int port = Integer.parseInt(args[4]);
				String sourceFolderName =args[5];
				String sourceFile = args[6];
				String retailer = args[7];
				String affiliator = args[8];
				String retailerno = args[10];
				String delimiter = args[9];
				String loadType = args[11];
				int s3flag = Integer.parseInt(args[12]);
				int mysqlflag = Integer.parseInt(args[13]);
				int esflag = Integer.parseInt(args[14]);
				deleteOffer.offerDelete(affiliator, retailer, retailerno);
			}
			else if((classtype.equals("nonupc2retailer")))
			{
				String hostName = args[1];
				String userName = args[2];
				String password = args[3];
				int port = Integer.parseInt(args[4]);
				String sourceFolderName =args[5];
				String sourceFile = args[6];
				String retailer = args[7];
				String affiliator = args[8];
				String retailerno = args[10];
				String delimiter = args[9];
				String loadType = args[11];
				int s3flag = Integer.parseInt(args[12]);
				int mysqlflag = Integer.parseInt(args[13]);
				int esflag = Integer.parseInt(args[14]);
				String stagingTableName=args[15];
				nonupcautomation.nonUpcAutomate(hostName, userName, password, port, sourceFolderName, sourceFile, retailer, affiliator, delimiter,
						retailerno,stagingTableName);
				
			}
			
			else 
			{
				
			}
		} catch (IOException e) {
			e.printStackTrace();
			// logger.info("IOException "+e.toString());
		}

	}

}
