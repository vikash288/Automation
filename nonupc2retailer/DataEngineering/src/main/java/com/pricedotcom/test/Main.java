package com.pricedotcom.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;

import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;

public class Main {
	
	private final MySqlOperation mysqlOpp = new MySqlOperation();
	private final MysqlConnection mysql = new MysqlConnection();	
	
	public void writeFile(String line) throws Exception{
		int counter=0;
		BufferedReader br=null;
			br = new BufferedReader(new FileReader(line));  
 		while ((line = br.readLine()) != null)
 		{      
 			String[] lines1 = line.split("\\|",-1);
 			
 			System.out.println(lines1.length);
		
	   
	}
	}
	
	
	public void upload() throws ClassNotFoundException, SQLException, IOException
	{
		String stagingTableName="PRICE_NOUPC_STG.STG_REALREAL_SHARESALE_DEMO_W";
		String dfile="/home/third-eye/DUMP/realreal1502875093.txt";
		
		long filecount = mysqlOpp.loadStagingData(mysql.connectMySQL(), "the real real", stagingTableName, "|",
				dfile, "");

	}
	
	
	
	public static void main(String args[])throws Exception{
		try{
			/*File file =new File("the-file-name.txt");
			PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
			
		    writer.println("The first line");
		    writer.println("The second line");
		    writer.close();
		    
		    Main main=new Main();
		    main.writeFile("/home/third-eye/DUMP/newshopping1502711553.txt");*/
		    Main main=new Main();
		    main.upload();
		//    main.writeFile("/home/third-eye/DUMP/realreal1502875093.txt");
			
		} catch (IOException e) {
		   // do something
		}
	}
	  
}


