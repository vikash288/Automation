package com.pricedotcom.elasticsearch;

import java.io.BufferedReader;
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
import com.pricedotcom.util.MiscellaneousUtil;
import com.pricedotcom.util.ProjectProperties;

public class NonUpcEsOperation {

	static ESOperations es = new ESOperations();
	static ProjectProperties prop = new ProjectProperties();
	static MysqlConnection conn = new MysqlConnection();
	private final static MiscellaneousUtil util = new MiscellaneousUtil();
	
	
	public void createIndex(String index, String jsonData) throws IOException {
		@SuppressWarnings("static-access")
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPUT", prop.getEslinknonupc() + prop.getEsindexnonupc() + Long.parseLong(index), "-d", jsonData);
		Process p = pb.start();
		InputStream is = p.getInputStream();
		String processString = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		while ((processString = br.readLine()) != null) {

			processString = processString + "\t" + br.readLine();
			System.out.println(br.readLine());
		}

	}
	
	
	
	@SuppressWarnings("static-access")
	public static void upsert(String index, Map<String, Object> up) throws IOException {

		Map<String, Object> updateJson = new HashMap<String, Object>();

		updateJson.put("doc", up);
		ObjectMapper mapper = new ObjectMapper();
		String stringjson = mapper.writeValueAsString(updateJson);
		System.out.println(stringjson);
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPOST",
				prop.getEslinknonupc() + prop.getEsindexnonupc() + Long.parseLong(index) + "/_update", "-d", stringjson);
		
		
		System.out.println("curl -XPOST '" + prop.getEslinknonupc() + prop.getEsindexnonupc() + "" + Long.parseLong(index)
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
		String query="";
		String DESCRIPTION="";
		String size="";
		String color="";

		MysqlConnection mysql = new MysqlConnection();
        
		Connection conn = mysql.connectMySQL();
		System.out.println("the retailer is ..."+retailerName);
		
		if(retailerName.equalsIgnoreCase("therealreal"))
		{
		
			query = " select FIELD_1 AS ITEM_ID,FIELD_2 AS TITLE,concat(203,FIELD_1) AS UPC,FIELD_12 AS DESCRIPTION,"
				+ "REPLACE(FIELD_5,'YOURUSERID','1272520') AS PRODUCT_URL,FIELD_7 AS IMAGE_URL,"
						+ "REPLACE(FIELD_5,'YOURUSERID','1272520')"
						+ " AS AFFILIATE_URL,FIELD_8 AS PRICE_1,FIELD_8 AS PRICE_2,"
						+ "FIELD_52 AS CAT_ID,FIELD_53 AS PARENT_CAT_ID,FIELD_54 AS PARENT_NAME,"
						+ "FIELD_55 AS CRUMB,FIELD_56 AS NAME FROM " + tableName;
		}
		else 
		{
			query = " select FIELD_1 AS ITEM_ID,FIELD_2 AS TITLE,FIELD_3 AS DESCRIPTION,concat(265,FIELD_1) AS UPC,"
					+ "FIELD_6 AS PRODUCT_URL,FIELD_8 AS IMAGE_URL,FIELD_7 AS AFFILIATE_URL,FIELD_17 as COLOR,FIELD_18 AS SIZE,"
					+ "replace(FIELD_11,'USD','') AS PRICE_1,replace(FIELD_12,'USD','') AS PRICE_2,"
							+ "FIELD_19 AS CAT_ID,FIELD_20 AS PARENT_CAT_ID,FIELD_21 AS PARENT_NAME,"
							+ "FIELD_22 AS CRUMB,FIELD_23 AS NAME FROM " + tableName;
		}
		
		System.out.println(query);
		Statement st = conn.createStatement();

		ResultSet rs = st.executeQuery(query);
		int i = 0;
		while (rs.next()) {
			Map<String, Object> rawJson = new HashMap<String, Object>();
			Map<String, Object> retailer = new HashMap<String, Object>();
			Map<String, Object> retailer1 = new HashMap<String, Object>();
			Map<String, Object> retailer2 = new HashMap<String, Object>();
			String TITLE = rs.getString("TITLE");
			DESCRIPTION=rs.getString("DESCRIPTION");
			String UPC = rs.getString("UPC");
			String PRODUCT_URL = rs.getString("PRODUCT_URL");
			String IMAGE_URL = rs.getString("IMAGE_URL");
			//String COMMONID = rs.getString("price_id");
			String affiliate_url = rs.getString("AFFILIATE_URL");
			String item_id = rs.getString("ITEM_ID");
			//String condition = rs.getString("PRODUCT_CONDITION");
			Double price1=rs.getDouble("PRICE_1");
			Double price2=rs.getDouble("PRICE_2");
			if(!retailerName.equalsIgnoreCase("therealreal"))
			{
			size=rs.getString("SIZE");
			color=rs.getString("COLOR");			
			}
			Double catid=Double.parseDouble(rs.getString("CAT_ID"));
			Double parentcatid=Double.parseDouble(rs.getString("PARENT_CAT_ID"));
			String pname=rs.getString("PARENT_NAME");
			String crumb=rs.getString("CRUMB");
			String name=rs.getString("NAME");
			
			if(PRODUCT_URL.contains("url=")){
				String[] link = PRODUCT_URL.split("url=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}else if(PRODUCT_URL.contains("u=")){
				String[] link = PRODUCT_URL.split("u=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}
			
			retailer.put("description", DESCRIPTION);
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
			rawJson.put("cat_id",catid);
			rawJson.put("parent_cat_id", parentcatid);
			rawJson.put("parent_name", pname);
			rawJson.put("crumb", crumb);
			rawJson.put("name", name);
			rawJson.put("size", size);
			rawJson.put("color", color);			
			//rawJson.put("price_id", Integer.parseInt(COMMONID));

			ObjectMapper mapper = new ObjectMapper();
			String stringjson = mapper.writeValueAsString(rawJson);
			System.out.println(stringjson);
			if (getData(UPC)) {
				retailer2.put("r", retailer1);
				upsert(UPC, retailer2);
			} else {
				es.createIndex(UPC, stringjson);
			}
			i++;
			System.out.println("Row no: " + (i));
		}
		rs.close();
		conn.close();
	}

	
	
	public static void getQuerys(String query, String retailerName)
			throws ClassNotFoundException, SQLException, IOException {

		MysqlConnection mysql = new MysqlConnection();
        
		Connection conn = mysql.connectMySQL();
		
		//String query = "select * from " + tableName;
		
		System.out.println(query);
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
				upsert(UPC, retailer2);
			} else {
				es.createIndex(UPC, stringjson);
			}
			i++;
			System.out.println("Row no: " + (i));
		}
		rs.close();
		conn.close();
	}

	
	
	
	
	// Curl Pattern for partial Delete
	// curl -XPOST
	// "http://search-price-used-production-wrobho4tskcnz4mxygweriempe.us-east-1.es.amazonaws.com/product_index/product/1/_update"
	// -d '{ "script": "ctx._source.r.remove(\"test1\")" }'

	@SuppressWarnings("static-access")
	public void partialDeleteCurl(String index, String key) {
	
		try {
			String removeScript = "{ \"script\": \"ctx._source.r.remove(\\\""+key+"\\\")\" }";
			@SuppressWarnings("static-access")
			ProcessBuilder pb = new ProcessBuilder("curl", "-XPOST",
					prop.getEslinknonupc() + prop.getEsindexnonupc() + Long.parseLong(index) + "/_update", "-d", removeScript);
			
			Process p = pb.start();
			InputStream is = p.getInputStream();
			String processString = "";
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			while ((processString = br.readLine()) != null) {

				processString = processString + "\t" + br.readLine();
				System.out.println(br.readLine());
			}

		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	public static boolean getData(String index) {
		boolean flag = false;
		try {
			@SuppressWarnings("static-access")
			String url = "http://"+prop.getEslinknonupc()+prop.getEsindexnonupc()
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

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		// TODO Auto-generated method stub
		NonUpcEsOperation esopp = new NonUpcEsOperation();
		/*if(esopp.getData("1")){
			System.out.println(esopp.getData("1"));
			esopp.partialDeleteCurl("1", "test2");
		}*/
		
		//esopp.getRows("PRICE_DOT_COM_TARGET.LINKSHARE_SAMSCLUB", "samsclub");
		esopp.getRows("PRICE_NOUPC_STG.STG_REALREAL_SHARESALE_DEMO_W", "therealreal");
		
		
		
	}

}
