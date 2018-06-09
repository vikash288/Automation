package com.pricedotcom.elasticsearch;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.util.ProjectProperties;

public class ElasticSearchClient {

	static ElasticSearchClient es = new ElasticSearchClient();
	static ProjectProperties prop = new ProjectProperties();
	static MysqlConnection conn = new MysqlConnection();


	public void createIndex(String index, String jsonData) throws IOException {
		@SuppressWarnings("static-access")
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPUT",
				prop.getEslink() + prop.getEsindex() + Long.parseLong(index), "-d", jsonData);
		Process p = pb.start();
		InputStream is = p.getInputStream();
		String processString = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		while ((processString = br.readLine()) != null) {

			processString = processString + "\t" + br.readLine();
			System.out.println(br.readLine());
		}

	}

	public void bulkLoad(String file) throws IOException {

		@SuppressWarnings("resource")
		InputStream inputStream = new FileInputStream(file);

		@SuppressWarnings("static-access")
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPUT",prop.getEslink()+"/_bulk --data-binary @"
						+ inputStream);
		Process p = pb.start();
		InputStream is = p.getInputStream();
		String processString = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		while ((processString = br.readLine()) != null) {

			processString = processString + "\t" + br.readLine();
			System.out.println(br.readLine());
		}

	}
	
	
	/* Sample Upsert Process
	 * curl -XPOST
	 * 'search-price-production-e6jhql3cozjbq2m4rdh6am27jy.us-east-1.es.
	 * amazonaws.com/product_index/product/12345/_update?pretty' -H
	 * 'Content-Type: application/json' -d' {
	 * "doc":{"r":{"bestbuy":{"product_url":"apple.com","image_url":
	 * "apple.com","title":"apple"}}}} '
	 * 
	 */

	@SuppressWarnings("static-access")
	public static void upsert(String index, Map<String, Object> up) throws IOException {

		Map<String, Object> updateJson = new HashMap<String, Object>();

		updateJson.put("doc", up);
		ObjectMapper mapper = new ObjectMapper();
		String stringjson = mapper.writeValueAsString(updateJson);
		System.out.println(stringjson);
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPOST",
				prop.getEslink() + prop.getEsindex() + Long.parseLong(index) + "/_update", "-d", stringjson);

		System.out.println("curl -XPOST '" + prop.getEslink() + prop.getEsindex() + "" + Long.parseLong(index)
				+ "/_update -d" + stringjson + "'");

		Process p = pb.start();
		InputStream is = p.getInputStream();
		String processString = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		while ((processString = br.readLine()) != null) {

			processString = processString + "\t" + br.readLine();
			System.out.println(br.readLine());
		}
	}


	public static void getRows(String tableName, String retailerName)
			throws ClassNotFoundException, SQLException, IOException {

		MysqlConnection mysql = new MysqlConnection();
        
		Connection conn = mysql.connectMySQL();
		
		String query = "select * from " + tableName +" limit 1";
		//String query = "select * from " + tableName;

		// String query = "select * from
		// PRICE_DOT_COM_TRGT.TRGT_TARGET_FEED_LINKSHARE_NEW where
		// upc=75678225826;";
		System.out.println(query);
		// create the java statement
		Statement st = conn.createStatement();

		ResultSet rs = st.executeQuery(query);
		int i = 0;
		while (rs.next()) {
			Map<String, Object> rawJson = new HashMap<String, Object>();
			Map<String, Object> retailer = new HashMap<String, Object>();
			Map<String, Object> retailer1 = new HashMap<String, Object>();
			Map<String, Object> retailer2 = new HashMap<String, Object>();
			String TITLE = rs.getString("title");
			String UPC = rs.getString("upc");
			String PRODUCT_URL = rs.getString("product_url");
			String IMAGE_URL = rs.getString("image_url");
			String COMMONID = rs.getString("price_id");
			String affiliate_url = rs.getString("AFFILIATE_URL");
			String item_id = rs.getString("ITEM_ID");
			//String condition = rs.getString("PRODUCT_CONDITION");
			Double price1=rs.getDouble("PRICE1");
			Double price2=rs.getDouble("PRICE2");
			
			if(PRODUCT_URL.contains("url=")){
				String[] link = PRODUCT_URL.split("url=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}else if(PRODUCT_URL.contains("u=")){
				String[] link = PRODUCT_URL.split("u=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}

			retailer.put("product_url", PRODUCT_URL);
			retailer.put("image_url", IMAGE_URL);
			retailer.put("title", TITLE);
			retailer.put("affiliate_url", affiliate_url);
			//retailer.put("condition", condition);
			retailer.put("price1", price1);
			retailer.put("price2", price2);
			retailer1.put(retailerName, retailer);
			rawJson.put("r", retailer1);
			rawJson.put("upc", Long.parseLong(UPC));
			rawJson.put("title", TITLE);
			rawJson.put("item_id", item_id);
			rawJson.put("price_id", Integer.parseInt(COMMONID));

			ObjectMapper mapper = new ObjectMapper();
			String stringjson = mapper.writeValueAsString(rawJson);
			System.out.println(stringjson);
			if (getData(UPC)) {
				retailer2.put("r", retailer1);
				//upsert(UPC, retailer2);
			} else {
				//es.createIndex(UPC, stringjson);
			}
			i++;
			System.out.println("Row no: " + (i));
		}
		rs.close();
		conn.close();
		System.exit(0);
		// iterate through the java resultset
	}

	public static void getRowsQuery(String query) throws ClassNotFoundException, SQLException, IOException {

		MysqlConnection mysql = new MysqlConnection();

		Connection conn = mysql.connectMySQL();

		// String query="select * from "+tableName;
		System.out.println(query);
		// create the java statement
		Statement st = conn.createStatement();

		ResultSet rs = st.executeQuery(query);
		while (rs.next()) {
			Map<String, Object> rawJson = new HashMap<String, Object>();
			Map<String, Object> retailer = new HashMap<String, Object>();
			Map<String, Object> retailer1 = new HashMap<String, Object>();
			Map<String, Object> retailer2 = new HashMap<String, Object>();
			String TITLE = rs.getString("TITLE");
			String UPC = rs.getString("UPC");
			String PRODUCT_URL = rs.getString("PRODUCT_URL");
			String IMAGE_URL = rs.getString("IMAGE_URL");
			String COMMONID = rs.getString("PRICE_ID");
			String affiliate_url = rs.getString("AFFILIATE_URL");
			String item_id = rs.getString("ITEM_ID");

			retailer.put("product_url", PRODUCT_URL);
			retailer.put("image_url", IMAGE_URL);
			retailer.put("title", TITLE);
			retailer.put("affiliate_url", affiliate_url);
			retailer1.put(rs.getString("tablename"), retailer);
			rawJson.put("r", retailer1);
			rawJson.put("upc", Long.parseLong(UPC));
			rawJson.put("title", TITLE);
			rawJson.put("item_id", item_id);
			rawJson.put("price_id", Integer.parseInt(COMMONID));

			ObjectMapper mapper = new ObjectMapper();
			String stringjson = mapper.writeValueAsString(rawJson);
			System.out.println(stringjson);
			if (getData(UPC)) {
				retailer2.put("r", retailer1);
				upsert(UPC, retailer2);
			} else {
				es.createIndex(UPC, stringjson);
			}
		}
		rs.close();
		conn.close();
		// iterate through the java resultset
	}

	public static boolean getData(String index) {
		boolean flag = false;
		try {
			@SuppressWarnings("static-access")
			String url = "http://"+prop.getEslink()+prop.getEsindex()
					+ Long.parseLong(index);
			URL stockJson = new URL(url); // URL
											// to
											// Parse
			URLConnection urlConn = stockJson.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));

			StringBuffer output = new StringBuffer();
			String inputLine;
			;
			while ((inputLine = in.readLine()) != null) {
				output.append(inputLine);
			}

			in.close();
			String json = output.toString();
			JSONObject obj = new JSONObject(json);
			flag = (boolean) obj.get("found");
			System.out.println(flag);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return flag;
	}
	
	
	
	public void ESbulkLoad(String tableName,int retailerNo){
		
		
		
	}
	

	public static void main(String args[]) throws IOException, ClassNotFoundException, SQLException {

		/*
		 * Map<String, Object> rawJson = new HashMap<String, Object>(); String
		 * index = "3"; Map<String, Object> json = new HashMap<String,
		 * Object>(); json.put("user", "kimchy"); json.put("postDate", new
		 * Date()); json.put("message", "trying out abhinandan Elasticsearch");
		 * 
		 * ObjectMapper mapper = new ObjectMapper(); String stringjson =
		 * mapper.writeValueAsString(json); System.out.println(stringjson);
		 * //es.createIndex(index, stringjson); es.getRows();
		 */
		// select * from
		// PRICE_DOT_COM_TRGT.TRGT_SIERRA_TRADING_POST_LINKSHARE_NEW where upc =
		// 190229008069
		// createESJSON();
		getRows("PRICE_DOT_COM_TARGET.CJ_PETCO", "test1");
		// getData("579344");
		// upsert("12345");

	}
}
