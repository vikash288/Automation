package com.pricedotcom.test;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.util.ProjectProperties;

public class ElasticSearchClientTest {

	static ElasticSearchClientTest es = new ElasticSearchClientTest();
	static ProjectProperties prop = new ProjectProperties();
	static MysqlConnection conn = new MysqlConnection();

	public void createIndex(String index, String jsonData) {
		@SuppressWarnings("static-access")
		ProcessBuilder pb;
		try {
			pb = new ProcessBuilder("curl", "-XPUT",
					prop.getUsedeslink() + prop.getUsedesindex() + Long.parseLong(index), "-d", jsonData);
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

	public void bulkLoad(String file) throws IOException {

		@SuppressWarnings("resource")
		InputStream inputStream = new FileInputStream(file);

		@SuppressWarnings("static-access")
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPUT",
				prop.getUsedeslink() + "/_bulk --data-binary @" + inputStream);
		Process p = pb.start();
		InputStream is = p.getInputStream();
		String processString = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		while ((processString = br.readLine()) != null) {

			processString = processString + "\t" + br.readLine();
			System.out.println(br.readLine());
		}

	}

	/*
	 * Sample Upsert Process curl -XPOST
	 * 'search-price-production-e6jhql3cozjbq2m4rdh6am27jy.us-east-1.es.
	 * amazonaws.com/product_index/product/12345/_update?pretty' -H
	 * 'Content-Type: application/json' -d' {
	 * "doc":{"r":{"bestbuy":{"product_url":"apple.com","image_url":
	 * "apple.com","title":"apple"}}}} '
	 * 
	 */

	@SuppressWarnings("static-access")
	public static void upsert(String index, Map<String, Object> up) {

		Map<String, Object> updateJson = new HashMap<String, Object>();

		updateJson.put("doc", up);
		ObjectMapper mapper = new ObjectMapper();
		String stringjson;
		try {
			stringjson = mapper.writeValueAsString(updateJson);
			System.out.println(stringjson);
			ProcessBuilder pb = new ProcessBuilder("curl", "-XPOST",
					prop.getUsedeslink() + prop.getUsedesindex() + Long.parseLong(index) + "/_update", "-d",
					stringjson);

			System.out.println("curl -XPOST '" + prop.getUsedeslink() + prop.getUsedesindex() + ""
					+ Long.parseLong(index) + "/_update -d" + stringjson + "'");

			Process p = pb.start();
			InputStream is = p.getInputStream();
			String processString = "";
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			while ((processString = br.readLine()) != null) {

				processString = processString + "\t" + br.readLine();
				System.out.println(br.readLine());
			}
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void getRowsMulti(String groupbyTableName, String tableName, String retailerName) {

		MysqlConnection mysql = new MysqlConnection();

		Connection conn = null;
		try {
			conn = mysql.connectMySQL();

			int i = 0;

			Statement st1 = conn.createStatement();

			String query1 = "select * from " + groupbyTableName + " where PRODUCT_CONDITION = 'Used' ";

			ResultSet rs1 = st1.executeQuery(query1);

			while (rs1.next()) {

				Map<String, Object> rawJson = new HashMap<String, Object>();
				Map<String, Object> retailer1 = new HashMap<String, Object>();
				Map<String, Object> retailer2 = new HashMap<String, Object>();

				String TITLE1 = "";
				String UPC1 = "";
				String item_id1 = "";
				String COMMONID1 = "";

				List<Object> arraylist = new ArrayList<Object>();

				String query = "select * from " + tableName + " where upc=" + rs1.getString("UPC")
						+ " and PRODUCT_CONDITION = 'Used'";

				// String query = "select * from " + tableName + " where
				// upc=617407675948 and PRODUCT_CONDITION = 'Used'";

				System.out.println(query);
				// create the java statement
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(query);

				while (rs.next()) {

					Map<String, Object> retailer = new HashMap<String, Object>();

					TITLE1 = rs.getString("title");
					UPC1 = rs.getString("upc");
					item_id1 = rs.getString("ITEM_ID");
					COMMONID1 = rs.getString("price_id");

					String CATEGORY = rs.getString("PRODUCT_CATEGORY");
					String TITLE = rs.getString("title");
					String UPC = rs.getString("upc");
					String PRODUCT_URL = rs.getString("product_url");
					String IMAGE_URL = rs.getString("image_url");
					String COMMONID = rs.getString("price_id");
					String affiliate_url = rs.getString("AFFILIATE_URL");
					String item_id = rs.getString("ITEM_ID");
					String condition = rs.getString("PRODUCT_CONDITION");
					Double price1 = rs.getDouble("PRICE1");
					Double price2 = rs.getDouble("PRICE2");
					String topseller = rs.getString("TOP_RATED_SELLER");
					String rating = rs.getString("SELLER_RATING_PERCENTAGE");

					if (CATEGORY.contains(":")) {
						String categoryarray[] = CATEGORY.split(":");
						for (int j = 0; j < categoryarray.length; j++) {
							retailer.put("categoty_" + (j + 1), categoryarray[j]);
						}
					} else {
						retailer.put("categoty_1", CATEGORY);
					}

					if (PRODUCT_URL.contains("url=")) {
						String[] link = PRODUCT_URL.split("url=");
						PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
					} else if (PRODUCT_URL.contains("u=")) {
						String[] link = PRODUCT_URL.split("u=");
						PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
					}

					retailer.put("item_id", item_id);
					retailer.put("product_url", PRODUCT_URL);
					retailer.put("image_url", IMAGE_URL);
					retailer.put("title", TITLE);
					retailer.put("affiliate_url", affiliate_url);
					retailer.put("condition", condition);
					retailer.put("price1", price1);
					retailer.put("price2", price2);
					retailer.put("top_seller", topseller);
					retailer.put("rating", rating);

					arraylist.add(retailer);

				}

				retailer1.put(retailerName, arraylist);
				rawJson.put("r", retailer1);
				rawJson.put("upc", Long.parseLong(UPC1));
				rawJson.put("title", TITLE1);
				// rawJson.put("item_id", item_id1);
				rawJson.put("price_id", COMMONID1);

				ObjectMapper mapper = new ObjectMapper();
				String stringjson = mapper.writeValueAsString(rawJson);
				System.out.println(stringjson);
				if (getData(UPC1)) {
					retailer2.put("r", retailer1);
					upsert(UPC1, retailer2);
				} else {
					es.createIndex(UPC1, stringjson);
				}
				i++;
				System.out.println("Row no: " + (i));

			}
		} catch (ClassNotFoundException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// iterate through the java resultset
	}

	public static void getRows(String tableName, String retailerName)
			throws ClassNotFoundException, SQLException, IOException {

		MysqlConnection mysql = new MysqlConnection();

		Connection conn = mysql.connectMySQL();

		// String query = "select * from " + tableName +" limit 5";
		String query = "select * from " + tableName;

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
			String condition = rs.getString("PRODUCT_CONDITION");
			Double price1 = rs.getDouble("PRICE1");
			Double price2 = rs.getDouble("PRICE2");
			String topseller = rs.getString("TOP_RATED_SELLER");
			Double rating = rs.getDouble("SELLER_RATING_PERCENTAGE");

			if (PRODUCT_URL.contains("url=")) {
				String[] link = PRODUCT_URL.split("url=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			} else if (PRODUCT_URL.contains("u=")) {
				String[] link = PRODUCT_URL.split("u=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}

			retailer.put("product_url", PRODUCT_URL);
			retailer.put("image_url", IMAGE_URL);
			retailer.put("title", TITLE);
			retailer.put("affiliate_url", affiliate_url);
			retailer.put("condition", condition);
			retailer.put("price1", price1);
			retailer.put("price2", price2);
			retailer1.put(retailerName, retailer);
			rawJson.put("r", retailer1);
			rawJson.put("upc", Long.parseLong(UPC));
			rawJson.put("title", TITLE);
			rawJson.put("item_id", item_id);
			rawJson.put("price_id", Integer.parseInt(COMMONID));
			retailer.put("top_seller", topseller);
			retailer.put("rating", rating);

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
		// iterate through the java resultset
	}

	public static boolean getData(String index) {
		boolean flag = false;
		try {
			@SuppressWarnings("static-access")
			String url = "http://" + prop.getUsedeslink() + prop.getUsedesindex() + Long.parseLong(index);
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

	public static void main(String args[])
			throws IOException, ClassNotFoundException, SQLException, InterruptedException {

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
		getRowsMulti("PRICE_DOT_COM_TRGT.TGT_EBAY_PETS1", "PRICE_DOT_COM_TRGT.TGT_EBAY_PETS1", "ebay");
		// getRows("PRICE_DOT_COM_TRGT.TGT_EBAY_ELEC_SINGLE", "ebay");
		// System.out.println(prop.getUsedeslink());
		// getData("579344");
		// upsert("12345");

	}
}
