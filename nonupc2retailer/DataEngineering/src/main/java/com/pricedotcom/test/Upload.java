package com.pricedotcom.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.json.JSONObject;

import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.restapi.PriceRestAPI;

public class Upload 
{
	private static String[] row = null;
	private static String filePath="";
	  String line = ""; 
	 
	//  String cvsSplitBy = "\\|";
      BufferedReader br = null;
      int c=0;
      private  final long unixTime = System.currentTimeMillis() / 1000L;
      private static final MysqlConnection mysqlCon = new MysqlConnection();
      private static  Statement st=null;
      private static  ResultSet rs=null;
      private static final  PriceRestAPI api=new PriceRestAPI();
     
     public  String  readCSV(String filepath,String cvsSplitBy)
     {	
    	 Connection conn=null;
    	 
    	 BufferedReader br = null;
	        String line = ""; 
	        String saveDir="/home/third-eye/DUMP";
	        filePath=saveDir + "/realreal" + unixTime + ".txt";
  		try {   
  	   
  	 	BufferedWriter bw = null;
		FileWriter fw = null;
    	 
       
     		br = new BufferedReader(new FileReader(filepath));  
     		while ((line = br.readLine()) != null)
     		{      
     		   			
     			String[] lines = line.split(cvsSplitBy,-1); 
     			
     			System.out.println(".........."+line); 			
     			
     					
     			conn=mysqlCon.connectMySQL();
     			String query="SELECT * FROM PRICE_DOT_COM_STAGING.PRODUCT_CATEGORY where PRODUCT_TITLE='"+lines[1]+"'";
     			st=conn.createStatement();
     			 rs=st.executeQuery(query);
     			if(rs.next())
     			{		
     				String csvRow=line+"|"+rs.getString("CATEGORY_ID");
     				    				
     			    PrintWriter writer = new PrintWriter(new FileWriter(filePath, true));
 				    writer.println(csvRow);
 				    writer.flush();     				  
 				    
     				row=null;
     				     				
     			}
     			else
     			{
     				
     				String json = api.getCatId(lines[1]).toString();
     				System.out.println(json);
     				    				
     				JSONObject obj = new JSONObject(json);
     				int id = (Integer) obj.get("cat_id");
     				int pid=(Integer) obj.get("parent_cat_id");
     				String pname=(String)obj.get("parent_name");
     				String crumb=obj.get("crumb").toString().replace("|", ":");     				
     				String name=(String)obj.get("name");        			    				
     				System.out.println(id);     			
     				String csvRow=line+"|"+id+"|"+pid+"|"+pname+"|"+crumb+"|"+name;
     				
     			   				
     				 PrintWriter writer = new PrintWriter(new FileWriter(filePath, true));
     				    writer.println(csvRow);
     				    writer.flush();     				  
     				    writer.close();
     				      				        				    	
     				    }
     		    	} 	
     		
     				
     		
     		} 		 catch (FileNotFoundException e)
		  				{   
		     			e.printStackTrace(); 
		  				}
		  				catch (IOException e) 
		  				{         
			     		e.printStackTrace();      
			     		} 
				  		catch (Exception e)
		  				{
				  			e.printStackTrace(); 
						}
  			return filepath;
     	}    
     
}
