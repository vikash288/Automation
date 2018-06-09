package com.pricedotcom.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.codec.binary.StringUtils;

import com.pricedotcom.mysql.MysqlConnection;

public class CleanProductURL {

	static MysqlConnection mysql = new MysqlConnection();
	static String tableName = "PRICE_DOT_COM_TRGT.TRGT_KMART_CJ_2";

	/*public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException {
				Connection conn = mysql.connectMySQL();

		String checkQuery = "select  count(*) as ITEM from " + tableName + " group by PRICE_ID having count(PRICE_ID) >1";
        System.out.println(checkQuery);
		String query = "select * from " + tableName;
		Statement st = conn.createStatement();
		Statement stmt = conn.createStatement();
		Statement checkQueryStmt = conn.createStatement();
		ResultSet checkQueryrs = checkQueryStmt.executeQuery(checkQuery);
		ResultSet rs = st.executeQuery(query);
		if(!checkQueryrs.isBeforeFirst()){
			System.out.println("Test");
		
			while (rs.next()) {
				String PRODUCT_URL = rs.getString("product_url");
				String ITEM_ID = rs.getString("item_id");

				if (PRODUCT_URL.contains("url=")) {
					String[] link = PRODUCT_URL.split("url=");
					PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8");

				}

				String sql = "UPDATE " + tableName + " SET PRODUCT_URL='" + PRODUCT_URL + "' where ITEM_ID='" + ITEM_ID
						+ "'";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				System.out.println("Data update sucessfully");
			}

		}else{
			System.out.println("Item ID Duplicated , table Name: "+tableName);
			System.exit(0);
		}
		rs.close();
		checkQueryrs.close();
		st.close();
		stmt.close();
		checkQueryStmt.close();
		conn.close();
	}*/
	
	
	
	
	
	public void cleanTable(String tableName) {
		Connection conn;
		try {
			conn = mysql.connectMySQL();
		

		String checkQuery = "select  count(*) as ITEM from " + tableName + " group by ITEM_ID having count(ITEM_ID) >1";
        System.out.println(checkQuery);
		String query = "select * from " + tableName;
		Statement st = conn.createStatement();
		Statement stmt = conn.createStatement();
		Statement checkQueryStmt = conn.createStatement();
		ResultSet checkQueryrs = checkQueryStmt.executeQuery(checkQuery);
		ResultSet rs = st.executeQuery(query);
		if(!checkQueryrs.isBeforeFirst()){
			System.out.println("Test");
		
			while (rs.next()) {
				String PRODUCT_URL = rs.getString("product_url");
				String ITEM_ID = rs.getString("item_id");

				if (PRODUCT_URL.contains("url=")) {
					String[] link = PRODUCT_URL.split("url=");
					PRODUCT_URL = java.net.URLDecoder.decode(link[1], "UTF-8").replace("'", "");
					
				}

				String sql = "UPDATE " + tableName + " SET PRODUCT_URL='" + PRODUCT_URL + "' where ITEM_ID='" + ITEM_ID
						+ "'";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				System.out.println("Data update sucessfully");
			}

		}else{
			System.out.println("Item ID Duplicated , table Name: "+tableName);
			System.exit(0);
		}
		
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
	
	
	public static void deleteTable() {
		Connection conn;
		try {
			conn = mysql.connectMySQL();

		String checkQuery = "select FIELD_11 from production.STG_MICHAELS_CJ_W where FIELD_11 <> ''  group by FIELD_11 Having count(FIELD_8) > 1 ";
        System.out.println(checkQuery);
		String query = checkQuery;
		Statement st = conn.createStatement();
		Statement stmt = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		int cnt=0;
			while (rs.next()) {
				String FIELD_11 = rs.getString("FIELD_11");
                System.out.println(FIELD_11);
				String sql = "delete FROM production.STG_MICHAELS_CJ_W_NONDUPLICATE  where FIELD_11='" +FIELD_11+ "'";
				System.out.println(sql);
				//System.exit(0);
				stmt.executeUpdate(sql);
				System.out.println("Data Delted sucessfully Rowno "+(cnt++));
			}
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
	
	public static void main(String args[]){
		deleteTable();
	}
	
	
	
	}

