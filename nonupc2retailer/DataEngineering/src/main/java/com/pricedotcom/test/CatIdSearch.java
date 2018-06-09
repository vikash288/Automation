package com.pricedotcom.test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricedotcom.pojo.ParentCat;
import com.pricedotcom.restapi.PriceRestAPI;

import au.com.bytecode.opencsv.CSVWriter;



public class CatIdSearch 
{
	 private static String filePath2="";
	 private static final long unixTime = System.currentTimeMillis() / 1000L;
	public static void main(String []args) {
		
		  
	String [] nextLine;
    try {
      //csv file containing data
      String strFile = "/home/third-eye/DUMP/googleshopping.csv";
           
      String thisLine;
      FileInputStream fis = new FileInputStream(strFile);
      DataInputStream myInput = new DataInputStream(fis);
      
      BufferedReader br =new BufferedReader(new FileReader(strFile));
      
      String line="";
      
      
           
     // int i = 1;
      String saveDir="/home/third-eye/DUMP";
      
      filePath2=saveDir + "/Data" + unixTime + ".csv";
      CSVWriter Data=new CSVWriter(new FileWriter(filePath2),CSVWriter.DEFAULT_SEPARATOR,
				CSVWriter.NO_QUOTE_CHARACTER);
		String DataHeader="product_title,Parent_Cat_id,Cat_id";
     // 	String DataHeader="product_title,st";
		Data.writeNext(DataHeader.split(","));
		Data.flush();
		
		List<String>shopLines=new ArrayList<String>();
	
		
		 /*  while ((line = br.readLine()) != null) {
			   shopLines.add(line);	
		   }
               // use comma as separator
               //String[] country = line.split(",");

              // System.out.println("Country [code= " + country[1]);
          for(int i=1;i<shopLines.size();i++)
          {
               String [] csv=shopLines.get((int)i).split(",");
               System.out.println("Country [code= " + csv[0]);
               
               String csvRow=csv[2]+","+i;
               System.out.println("Country [code= " + csvRow);
       		Data.writeNext(csvRow.split(","));
       		Data.flush();

           
          }*/
		
		
		
    while ((thisLine = myInput.readLine()) != null) {
          String product[] = thisLine.split(",");
          JSONParser parser = new JSONParser();
          //    System.out.println(strar[1]);
             PriceRestAPI api=new PriceRestAPI();
   
		String product_title=product[1].toString();
		System.out.println("PRODUCT TITLE................."+product_title);
		
		String json = api.getCatId(product[1]).toString();
		
		JSONObject obj = new JSONObject(json);
		int id = (Integer) obj.get("cat_id");
			System.out.println("catId.........."+id);
		
		int parent_id=(Integer)obj.get("parent_cat_id");
		
		//System.out.println("parent catId.........."+parent_id);
		
		String st=product[1].toString();
		
		System.out.println("parent catId.........."+st);
		
		String csvRow=product[1]+","+parent_id+","+id+"\n";
	//	String csvRow=product_title;
	//	System.out.println("..............."+csvRow);
	//	Data.writeNext(csvRow.split(","));
		Data.flush();
		}
	
  } catch (Exception e) {
		// TODO: handle exception
	}
  }
	
	

}
