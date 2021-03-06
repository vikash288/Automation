package com.pricedotcom.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

public class EmailUtil {
	private final String USER_AGENT = "Mozilla/5.0";

	public void sendMailGet(String body) {

		try {
			StringBuilder stringBuilder = new StringBuilder("http://production12.getpriceapp.com/data/notify/");
			stringBuilder.append("?body=");
			stringBuilder.append(URLEncoder.encode(body, "UTF-8"));

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

	}

	public void sendMailGet(String subject, String body) {
		try {
			CloseableHttpClient client = HttpClients.createDefault();
			HttpPost httpPost = new HttpPost("http://production12.getpriceapp.com/data/notify/");

			List<NameValuePair> params = new ArrayList<NameValuePair>();
			params.add(new BasicNameValuePair("subject", subject));
			params.add(new BasicNameValuePair("body", body));

			httpPost.setEntity(new UrlEncodedFormEntity(params));

			CloseableHttpResponse response = client.execute(httpPost);
			System.out.println(response.getStatusLine().getStatusCode());
			try {
				client.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * try { StringBuilder stringBuilder = new
		 * StringBuilder("http://production12.getpriceapp.com/data/notify/");
		 * stringBuilder.append("?body=");
		 * stringBuilder.append(URLEncoder.encode(body, "UTF-8"));
		 * stringBuilder.append("&subject="+URLEncoder.encode(subject,
		 * "UTF-8"));
		 * 
		 * 
		 * System.out.println(stringBuilder.toString());
		 * 
		 * URL url = new URL(stringBuilder.toString()); HttpURLConnection con =
		 * (HttpURLConnection) url.openConnection();
		 * 
		 * // By default it is GET request con.setRequestMethod("GET");
		 * 
		 * // add request header con.setRequestProperty("User-Agent",
		 * USER_AGENT);
		 * 
		 * int responseCode = con.getResponseCode(); System.out.println(
		 * "Sending get request : " + url); System.out.println(
		 * "Response code : " + responseCode);
		 * 
		 * // Reading response from input Stream BufferedReader in = new
		 * BufferedReader(new InputStreamReader(con.getInputStream())); String
		 * output; StringBuffer response = new StringBuffer();
		 * 
		 * while ((output = in.readLine()) != null) { response.append(output); }
		 * in.close();
		 * 
		 * // printing result from response
		 * System.out.println(response.toString()); } catch
		 * (UnsupportedEncodingException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (MalformedURLException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); } catch (IOException
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); }
		 */

	}

	public static void main(String args[]) throws IOException {
		LogUtil log = new LogUtil();
		EmailUtil email = new EmailUtil();
		String content = log.readLogFile("ebay", "/home/abhinandan/Projects/Price.com/dump/shell/ebay_2017-08-12.log");
		email.sendMailGet("Automation load process complete for Ebay", content);
		String userName = "abhinadnan";
		userName = userName.substring(0, 1).toUpperCase() + userName.substring(1).toLowerCase();
		System.out.println(userName);

	}

}