package com.pricedotcom.restapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

public class PriceRestAPI {
	
	private final String USER_AGENT = "Mozilla/5.0";
	
	public String getUPC(String asin){
		String responseText ="";
		try {
			StringBuilder stringBuilder = new StringBuilder("http://production12.getpriceapp.com/v2/price/asin-to-upc/");
			stringBuilder.append("?asin=");
			stringBuilder.append(URLEncoder.encode(asin, "UTF-8"));

			System.out.println(stringBuilder.toString());

			URL url = new URL(stringBuilder.toString());
			HttpURLConnection con = (HttpURLConnection) url.openConnection();

			// By default it is GET request
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
			System.out.println("Sending get request : " + url);
			System.out.println("Response code : " + responseCode);

			// Reading response from input Stream
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String output;
			StringBuffer response = new StringBuffer();

			while ((output = in.readLine()) != null) {
				response.append(output);
			}
			in.close();

			// printing result from response
			System.out.println(response.toString());
			responseText=response.toString();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return responseText;
	} 
	
	
	
	
	
	
	
	public String getCatId(String productTitle){
		String responseText ="";
		try {
			StringBuilder stringBuilder = new StringBuilder("http://production12.getpriceapp.com/v2/price/title_cat_search/");
			stringBuilder.append("?q=");
			stringBuilder.append(URLEncoder.encode(productTitle, "UTF-8"));

			System.out.println(stringBuilder.toString());

			URL url = new URL(stringBuilder.toString());
			HttpURLConnection con = (HttpURLConnection) url.openConnection();

			// By default it is GET request
			con.setRequestMethod("GET");

			// add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
			System.out.println("Sending get request : " + url);
			System.out.println("Response code : " + responseCode);

			// Reading response from input Stream
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String output;
			StringBuffer response = new StringBuffer();

			while ((output = in.readLine()) != null) {
				response.append(output);
			}
			in.close();

			// printing result from response
			System.out.println(response.toString());
			responseText=response.toString();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return responseText;
	}

}
