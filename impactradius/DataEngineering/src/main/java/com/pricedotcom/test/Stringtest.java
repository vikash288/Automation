package com.pricedotcom.test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Stringtest {
	
	public static void main(String args[]){
		/*int reviewcnt=9;
		String reviewJSON  = "{\"script\" : \"ctx._source.r.ebay.review ="+reviewcnt+"\"}";
		String ratingJSON = "{\"script\" : \"ctx._source.rating ="+reviewJSON+"\"}";
		String ebay = "{\"condition\": \"New\"}";
		
		String a ="{\"doc\" : {\"r\":{ \"ebay\" : [ "+ebay+" ] } }}";
		
		System.out.println(ratingJSON);
		System.out.println(a);*/
		SimpleDateFormat df = new SimpleDateFormat("Y-MM-d");
		System.out.println(df.format(new Date()));
	}

}
