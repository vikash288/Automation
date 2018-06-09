package com.pricedotcom.nonupc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.json.JSONObject;

import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.mysql.MysqlNonUpcOperation;
import com.pricedotcom.restapi.PriceRestAPI;

public class AutomationNonUPC {

	PriceRestAPI restapi = new PriceRestAPI();
	MysqlConnection mysqlconn = new MysqlConnection();
	MarchentFTPConnection marchentFTP =new MarchentFTPConnection();
	private final MysqlNonUpcOperation nonupcMysqlOpp = new MysqlNonUpcOperation();

	static String csvFile = "/home/abhinandan/Projects/Price.com/dump/googleshopping.csv";
	static String filepath = "/home/abhinandan/Projects/Price.com/dump/test";
	BufferedReader br = null;
	String line = "";
	String cvsSplitBy = "\\|,-1";
	
	
	public String downloadFile(String URL) throws IOException{
		String a = marchentFTP.httpDownloadFileS3("https://snobswap.com/googleshopping.csv", "snobswap");
		return a;
	}
	
	

	public void readCSV(String filepath) {
    
		try {
            int i = 0;
            Connection conn = mysqlconn.connectMySQL();
			PrintWriter writer = new PrintWriter("/home/abhinandan/Projects/Price.com/dump/samplegoogleshopping.csv", "UTF-8");
			br = new BufferedReader(new FileReader(filepath));
			br.readLine();
			while ((line = br.readLine()) != null) {
                i++;
				// use comma as separator
				String[] lines = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				System.out.println("old line: " + lines.length);
				String jsonData = restapi.getCatId(lines[1].replace("\"", ""));
				// {"parent_cat_id": 1, "parent_name": "Home", "cat_id": 25459,
				// "crumb": "Uncategorized|Home", "name": "Uncategorized"}

				JSONObject obj = new JSONObject(jsonData);

				String parent_cat_id = obj.get("parent_cat_id").toString();
				String parent_name = obj.get("parent_name").toString();
				String cat_id = obj.get("cat_id").toString();
				String crumb = obj.get("crumb").toString();
				String name = obj.get("name").toString();

				String newLine = line + "," + "\"" + parent_cat_id + "\"," + "\"" + parent_name + "\"," + "\""
						+ cat_id + "\"," + "\"" + crumb + "\"," +"\""+ name + "\"";
				
				System.out.println(newLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").length);

				System.out.println(newLine);
				writer.println(newLine);
				
				
				if(i==10){
					writer.flush();
					writer.close();
					long cnt =nonupcMysqlOpp.loadNonUpcStagingData(conn, "snobswap", "PRICE_NOUPC_STG.STG_SNOBSWAP_DEMO_W", ",", "/home/abhinandan/Projects/Price.com/dump/samplegoogleshopping.csv");
					System.out.println("Straging table line cnt: "+cnt);
					System.exit(0);
					
				}

				

			}
			
			

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (br != null) {
;				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void main(String args[]) throws IOException {

		AutomationNonUPC nonupc = new AutomationNonUPC();
		//nonupc.readCSV(csvFile);
		System.out.println(nonupc.downloadFile(""));

	}

}
