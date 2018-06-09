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
import com.pricedotcom.util.ProjectProperties;

import com.algolia.search.APIClient;
import com.algolia.search.ApacheAPIClientBuilder;
import com.algolia.search.Index;
import com.algolia.search.exceptions.AlgoliaException;
import com.algolia.search.objects.Query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class ESOperations {

	static ESOperations es = new ESOperations();
	static ProjectProperties prop = new ProjectProperties();
	static MysqlConnection conn = new MysqlConnection();
	
	
	
	public void createIndex(String index, String jsonData) throws IOException {
		@SuppressWarnings("static-access")
		ProcessBuilder pb = new ProcessBuilder("curl", "-XPUT", prop.getEslinkauto() + prop.getEsindexauto() + Long.parseLong(index), "-d", jsonData);
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
				prop.getEslinkauto() + prop.getEsindexauto() + Long.parseLong(index) + "/_update", "-d", stringjson);

		System.out.println("curl -XPOST '" + prop.getEslinkauto() + prop.getEsindexauto() + "" + Long.parseLong(index)
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
	
	public static void Algoliaupsert(String initIndex, ObjectNode json ) throws IOException {
		
		try {
		
		APIClient client = new ApacheAPIClientBuilder(prop.getAlgoliaAccessKey(), prop.getAlgoliaSecretKey()).build();
		 
		Index index = client.initIndex(initIndex);
		index.addObject(json);
		System.out.println("Upsert completed");
		}
		catch (AlgoliaException e) {
			e.printStackTrace();
		}
		 
	}
		
	public static void getRows(String tableName, String retailerName)
			throws ClassNotFoundException, SQLException, IOException {

		MysqlConnection mysql = new MysqlConnection();
        
		Connection conn = mysql.connectMySQL();
		
		String query = "select * from " + tableName;
		
		System.out.println(query);
		Statement st = conn.createStatement();

		ResultSet rs = st.executeQuery(query);
		int i = 0;
		while (rs.next()) {
			Map<String, Object> rawJson = new HashMap<String, Object>();
			Map<String, Object> retailer = new HashMap<String, Object>();
			Map<String, Object> retailer1 = new HashMap<String, Object>();
			Map<String, Object> retailer2 = new HashMap<String, Object>();
			//Algolia Object
			ObjectNode jsonAlgolia = JsonNodeFactory.instance.objectNode();

			String TITLE = rs.getString("title");
			String DESCRIPTION = rs.getString("DESCRIPTION");
			String COLOR = rs.getString("COLOR");
			String SIZE = rs.getString("SIZE");
			String UPC = rs.getString("upc");
			String PRODUCT_URL = rs.getString("product_url");
			String IMAGE_URL = rs.getString("image_url");
			String COMMONID = rs.getString("price_id");
			String affiliate_url = rs.getString("AFFILIATE_URL");
			String item_id = rs.getString("ITEM_ID");
			//String condition = rs.getString("PRODUCT_CONDITION");
			Double price1=rs.getDouble("PRICE1");
			Double price2=rs.getDouble("PRICE2");
			String sku_condition=rs.getString("SKU_CONDITION");
                        String mpn=rs.getString("MPN");
                        String manufacturer=rs.getString("MANUFACTURER");
                        String keywords=rs.getString("KEYWORDS");
                        String category_breadcrumb=rs.getString("CATEGORY_BREADCRUMB");
                        String gender=rs.getString("GENDER");
                       // String brand=rs.getString("BRAND");
			
					
			if(PRODUCT_URL.contains("url=")){
				String[] link = PRODUCT_URL.split("url=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}else if(PRODUCT_URL.contains("u=")){
				String[] link = PRODUCT_URL.split("u=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}

			retailer.put("title", TITLE);
			retailer.put("description", DESCRIPTION);
			//retailer.put("color", COLOR);
			//retailer.put("size", SIZE);
			retailer.put("product_url", PRODUCT_URL);
			retailer.put("image_url", IMAGE_URL);
			retailer.put("affiliate_url", affiliate_url);
			//retailer.put("condition", condition);
			retailer.put("price1", price1);
			retailer.put("price2", price2);
			retailer.put("sku_condition",sku_condition);
			retailer1.put(retailerName, retailer);
			rawJson.put("r", retailer1);
			rawJson.put("upc", Long.parseLong(UPC));
			rawJson.put("title", TITLE);
			rawJson.put("item_id", item_id);
		    rawJson.put("mpn", mpn);
            rawJson.put("sku_manufacturer", manufacturer);
            rawJson.put("keywords", keywords);
            rawJson.put("category_breadcrumb", category_breadcrumb);
            rawJson.put("gender", gender);
            //rawJson.put("sku_brand", brand);
            rawJson.put("color", COLOR);
            rawJson.put("size", SIZE);
            rawJson.put("price_id", Integer.parseInt(COMMONID));
			
            
            //Algolia Json 
			jsonAlgolia.put("description", DESCRIPTION);
			jsonAlgolia.put("objectID", Integer.parseInt(COMMONID));
			jsonAlgolia.put("color", COLOR);
			jsonAlgolia.put("product_url", PRODUCT_URL);
			jsonAlgolia.put("price2",price1);
			jsonAlgolia.put("price1",price2);
			jsonAlgolia.put("upc", Long.parseLong(UPC));
			jsonAlgolia.put("image_url", IMAGE_URL);
			jsonAlgolia.put("manufacturer",manufacturer);  
			jsonAlgolia.put("mpn",mpn);   
			jsonAlgolia.put("category_breadcrumb" , category_breadcrumb);   
			jsonAlgolia.put("keywords",keywords);
			jsonAlgolia.put("gender",gender);
			jsonAlgolia.put("size",SIZE);
		
			
	
			ObjectMapper mapper = new ObjectMapper();
			String stringjson = mapper.writeValueAsString(rawJson);
			System.out.println(stringjson);
			if (getData(UPC)) {
				retailer2.put("r", retailer1);
				//The commit add as need to upsert json 
				 //upsert(UPC, retailer2);
				upsert(UPC, rawJson);
			} else {
				es.createIndex(UPC, stringjson);
			}
			//Algolia upsert call
			Algoliaupsert(prop.getAlgoliaIndex(),jsonAlgolia);
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
			//Algolia Object
			ObjectNode jsonAlgolia = JsonNodeFactory.instance.objectNode();

			String TITLE = rs.getString("title");
			String DESCRIPTION = rs.getString("DESCRIPTION");
			String COLOR = rs.getString("COLOR");
                        String SIZE = rs.getString("SIZE");
			String UPC = rs.getString("upc");
			String PRODUCT_URL = rs.getString("product_url");
			String IMAGE_URL = rs.getString("image_url");
			String COMMONID = rs.getString("price_id");
			String affiliate_url = rs.getString("AFFILIATE_URL");
			String item_id = rs.getString("ITEM_ID");
			//String condition = rs.getString("PRODUCT_CONDITION");
			Double price1=rs.getDouble("PRICE1");
			Double price2=rs.getDouble("PRICE2");
			String sku_condition=rs.getString("SKU_CONDITION");
            String mpn=rs.getString("MPN");
            String manufacturer=rs.getString("MANUFACTURER");
            String keywords=rs.getString("KEYWORDS");
            String category_breadcrumb=rs.getString("CATEGORY_BREADCRUMB");
            String gender=rs.getString("GENDER");
            //String brand=rs.getString("BRAND");

            		


			
			if(PRODUCT_URL.contains("url=	")){
				String[] link = PRODUCT_URL.split("url=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}else if(PRODUCT_URL.contains("u=")){
				String[] link = PRODUCT_URL.split("u=");
				PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");
			}
			
			if(DESCRIPTION != null && !DESCRIPTION.isEmpty()){
				//DESCRIPTION="No Description Found";
			}else{
				DESCRIPTION="No Description Found";
			}

			retailer.put("product_url", PRODUCT_URL);
			retailer.put("image_url", IMAGE_URL);
			retailer.put("title", TITLE);
			retailer.put("description", DESCRIPTION);
			retailer.put("affiliate_url", affiliate_url);
			//retailer.put("condition", condition);
			retailer.put("price1", price1);
			retailer.put("price2", price2);
            retailer.put("sku_condition",sku_condition);
			retailer1.put(retailerName, retailer);
			rawJson.put("r", retailer1);
			rawJson.put("upc", Long.parseLong(UPC));
			rawJson.put("title", TITLE);
			rawJson.put("item_id", item_id);
			rawJson.put("mpn", mpn);
            rawJson.put("sku_manufacturer", manufacturer);
            rawJson.put("keywords", keywords);
            rawJson.put("category_breadcrumb", category_breadcrumb);
            rawJson.put("gender", gender);
            //rawJson.put("sku_brand", brand);
            rawJson.put("color", COLOR);
            rawJson.put("size", SIZE);
			rawJson.put("price_id", Integer.parseInt(COMMONID));
			
			
            
            //Algolia Json 
			jsonAlgolia.put("description", DESCRIPTION);
			jsonAlgolia.put("objectID", Integer.parseInt(COMMONID));
			jsonAlgolia.put("color", COLOR);
			jsonAlgolia.put("product_url", PRODUCT_URL);
			jsonAlgolia.put("price2",price1);
			jsonAlgolia.put("price1",price2);
			jsonAlgolia.put("upc", Long.parseLong(UPC));
			jsonAlgolia.put("image_url", IMAGE_URL);
			jsonAlgolia.put("manufacturer",manufacturer);  
			jsonAlgolia.put("mpn",mpn);   
			jsonAlgolia.put("category_breadcrumb" , category_breadcrumb);   
			jsonAlgolia.put("keywords",keywords);
			jsonAlgolia.put("gender",gender);
			jsonAlgolia.put("size",SIZE);
			
			

			ObjectMapper mapper = new ObjectMapper();
			String stringjson = mapper.writeValueAsString(rawJson);
			System.out.println(stringjson);
			if (getData(UPC)) {
				retailer2.put("r", retailer1);
				///upsert(UPC, retailer2);
				upsert(UPC, rawJson);
			} else {
				es.createIndex(UPC, stringjson);
			}
			
			//Algolia upsert call
			Algoliaupsert(prop.getAlgoliaIndex(),jsonAlgolia);
			
			
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
					prop.getEslinkauto() + prop.getEsindexauto() + Long.parseLong(index) + "/_update", "-d", removeScript);
			
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
	public static void AlgoliaDelete(String initIndex , String ObjectID) throws IOException {
		
		try {
		
		APIClient client = new ApacheAPIClientBuilder(prop.getAlgoliaAccessKey(), prop.getAlgoliaSecretKey()).build();
		Index index = client.initIndex(initIndex);
		index.deleteObject(ObjectID);
		System.out.println("Delete completed");
		}
		catch (AlgoliaException e) {
			e.printStackTrace();
		}
		 
	}
	
	
	
	public static boolean getData(String index) {
		boolean flag = false;
		try {
			@SuppressWarnings("static-access")
			String url = "http://"+prop.getEslinkauto()+prop.getEsindexauto()
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
		ESOperations esopp = new ESOperations();
		/*if(esopp.getData("1")){
			System.out.println(esopp.getData("1"));
			esopp.partialDeleteCurl("1", "test2");
		}*/
		
		//esopp.getRows("PRICE_DOT_COM_TARGET.LINKSHARE_SAMSCLUB", "samsclub");
		esopp.getRows("PRICE_DOT_COM_TARGET.AMAZON_DAILY","amazon");
		
		
		
	}

}
