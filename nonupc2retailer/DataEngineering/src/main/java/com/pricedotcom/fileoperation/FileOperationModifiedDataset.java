package com.pricedotcom.fileoperation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.json.JSONObject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.pricedotcom.marchentftp.MarchentFTPConnection;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.restapi.PriceRestAPI;
import com.pricedotcom.test.Main;
import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * @author third-eye
 *
 */
public class FileOperationModifiedDataset {
	private static String[] row = null;
	//private static String csvFilename = "/home/third-eye/DUMP/googleshopping.csv";
	// private static String csvFilename =
	// "/home/third-eye/pricedotcom/32926.txt";
	// private static String csvFilename = "/home/third-eye/DUMP/32926.txt";

	private static final MysqlConnection mysqlCon = new MysqlConnection();
	private static final PriceRestAPI api = new PriceRestAPI();

	private static String filePath = "";
	private static String filePathSecond = "";
	private static final long unixTime = System.currentTimeMillis() / 1000L;

	private static Connection conn = null;
	private static Statement st = null;
	private static ResultSet rs = null;
	private static Main filewrite = new Main();

	private final MysqlConnection mysql = new MysqlConnection();
	private final MarchentFTPConnection ftp = new MarchentFTPConnection();
	private final MiscellaneousUtil util = new MiscellaneousUtil();
	private final ProjectProperties prop = new ProjectProperties();
	private final LogUtil logutil = new LogUtil();
	private final MySqlOperation mysqlOpp = new MySqlOperation();

	public static void insertData(String tableName, String fieldseparator) {
		// String fieldseparator=",";
		try {
			conn = mysqlCon.connectMySQL();
			String countQuery = "select count(*) from " + tableName;
			String fileLoad = "load data local infile '" + filePath + "' into table " + tableName
					+ " fields terminated by '" + fieldseparator + "'";

			st.execute(fileLoad);
			/*
			 * rs = st.executeQuery(countQuery); rs.next(); String rowcnt =
			 * rs.getString(1);
			 */

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	String line = "";
	BufferedReader br = null;
	int c = 0;

	public List<String> readCSV(String filepath, String retailor, String cvsSplitBy) throws IOException {
		List<String> files = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";
		String saveDir = prop.getDumpfilepath();
	//	 String saveDir="/home/third-eye/DUMP";
		filePath = saveDir + "/" + retailor + unixTime + ".txt";
		filePathSecond = saveDir + "/" + retailor + "json" + unixTime + ".txt";

		try {

			BufferedWriter bw = null;
			FileWriter fw = null;

			br = new BufferedReader(new FileReader(filepath));
			while ((line = br.readLine()) != null) {

				String[] lines = line.split(cvsSplitBy, -1);

				conn = mysqlCon.connectMySQL();
				/*String query = "SELECT * FROM PRICE_DOT_COM_STAGING.PRODUCT_CATEGORY where PRODUCT_TITLE='" + lines[1]
						+ "'";*/
				String query = "SELECT * FROM PRICE_DOT_COM_STAGING.PRODUCT_CATEGORY where PRODUCT_TITLE='"
						+ lines[1].replace("'", "''") + "'";
				
				System.out.println("query is ....."+query);
				
				st = conn.createStatement();
				rs = st.executeQuery(query);
				if (rs.next()) {
					String csvRow = line + "|" + rs.getString("CATEGORY_ID")+"|"+rs.getString("CATEGORY_ID")
							+"|" + rs.getString("PARENT_CAT_ID")+"|" + rs.getString("PARENT_NAME")+
							"|" + rs.getString("CRUMB")+"|" + rs.getString("NAME");
							
					String csvRowTitle = lines[1] + "|" + rs.getString("CATEGORY_ID") + "|"
							+ rs.getString("PARENT_CAT_ID") + "|" + rs.getString("PARENT_NAME") + "|"
							+ rs.getString("CRUMB") + "|" + rs.getString("NAME");

					//PrintWriter writerjson = new PrintWriter(new FileWriter(filePathSecond, true));
					PrintWriter writer = new PrintWriter(new FileWriter(filePath, true));
					writer.println(csvRow);
					//writerjson.println(csvRowTitle);

					//writerjson.flush();
					writer.flush();

					row = null;
					
					
					

				} else {

					String json = api.getCatId(lines[1]).toString();
					System.out.println(json);

					JSONObject obj = new JSONObject(json);
					int id = (Integer) obj.get("cat_id");
					int pid = (Integer) obj.get("parent_cat_id");
					String pname = (String) obj.get("parent_name");
					String crumb = obj.get("crumb").toString().replace("|", ":");
					String name = (String) obj.get("name");
					System.out.println(id);
					String csvRow = line + "|" + id + "|" + pid + "|" + pname + "|" + crumb + "|" + name;
					String csvRowTitle = lines[1] + "|" + id + "|" + pid + "|" + pname + "|" + crumb + "|" + name;

					PrintWriter writer = new PrintWriter(new FileWriter(filePath, true));
					PrintWriter writerjson = new PrintWriter(new FileWriter(filePathSecond, true));
					writer.println(csvRow);
					writerjson.println(csvRowTitle);
					writer.flush();
					writerjson.flush();
				
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		files.add(filePath);
		files.add(filePathSecond);
		return files;

	}

	public List<String> read(String filepath, String retailor, String cvsSplitBy) throws IOException {
		List<String> files = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";
		 String saveDir=prop.getDumpfilepath();
		//String saveDir = "/home/third-eye/DUMP";
		filePath = saveDir + "/" + retailor + unixTime + ".txt";
		filePathSecond = saveDir + "/" + retailor + "json" + unixTime + ".txt";

		try {

			BufferedWriter bw = null;
			FileWriter fw = null;
			int flag = 0;

			br = new BufferedReader(new FileReader(filepath));
			while ((line = br.readLine()) != null) {
				flag++;
				if (flag > 1) {

					String[] lines = line.split(cvsSplitBy, -1);

					conn = mysqlCon.connectMySQL();

					String query = "SELECT * FROM PRICE_DOT_COM_STAGING.PRODUCT_CATEGORY where PRODUCT_TITLE='"
							+ lines[1].replace("'", "''") + "'";
					System.out.println("query is....." + query);
					st = conn.createStatement();
					rs = st.executeQuery(query);
					if (rs.next()) {
						String csvRow = line + ",\"" + rs.getString("CATEGORY_ID") + "\",\""
								+ rs.getString("PARENT_CAT_ID") + "\",\"" + rs.getString("PARENT_NAME") + "\",\""
								+ rs.getString("CRUMB") + "\",\"" + rs.getString("NAME") + "\"";
						
						String csvRowTitle = lines[1] + ",\"" + rs.getString("CATEGORY_ID") + "\",\""
								+ rs.getString("PARENT_CAT_ID") + "\",\"" + rs.getString("PARENT_NAME") + "\",\""
								+ rs.getString("CRUMB") + "\",\"" + rs.getString("NAME") + "\"";

						PrintWriter writer = new PrintWriter(new FileWriter(filePath, true));
						//PrintWriter writerjson = new PrintWriter(new FileWriter(filePathSecond, true));
						 writer.println(csvRow);
						//writerjson.println(csvRowTitle);
						writer.flush();
						//writerjson.flush();
						

						row = null;

					} else {

						String json = api.getCatId(lines[1].replace("\"", ""));
						System.out.println(json);

						JSONObject obj = new JSONObject(json);
						int id = (Integer) obj.get("cat_id");
						int pid = (Integer) obj.get("parent_cat_id");
						String pname = (String) obj.get("parent_name");
						String crumb = obj.get("crumb").toString().replace("|", ":");
						String name = (String) obj.get("name");
						System.out.println(id);
						String csvRow = line + ",\"" + id + "\",\"" + pid + "\",\"" + pname + "\",\"" + crumb + "\",\""
								+ name + "\"";
						String csvRowTitle = lines[1] + ",\"" + id + "\",\"" + pid + "\",\"" + pname + "\",\"" + crumb
								+ "\",\"" + name + "\"";

						PrintWriter writer = new PrintWriter(new FileWriter(filePath, true));
						PrintWriter writerjson = new PrintWriter(new FileWriter(filePathSecond, true));
						writer.println(csvRow);
						writerjson.println(csvRowTitle);
						writer.flush();
						writerjson.flush();
						

					}
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		files.add(filePath);
		files.add(filePathSecond);
		return files;

	}

	public static void main(String[] args) {
		FileOperationModifiedDataset dataset = new FileOperationModifiedDataset();

		// dataset.readCSV(csvFilename,"realreal",",");
		// dataset.read(csvFilename,"realreal",",");

	}

}
