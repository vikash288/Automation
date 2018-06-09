package com.pricedotcom.test;

import java.io.UnsupportedEncodingException;

public class FindUrl {

	public static void main(String args[]) throws UnsupportedEncodingException {
		//String a = "http://www.tkqlhce.com/click-8030100-10660381?url=http%3A%2F%2Fwww.kmart.com%2Fcold-steel-irish-blackthorn-91pbs%2Fp-00642174000P";
		/*String a = "http://www.tkqlhce.com/click-8030100-10660381?url=http%3A%2F%2Fwww.kmart.com%2Fshimano-saguaro-spin-rod-7%27%2Fp-00662566000P";
        
		if(a.contains("url=")){
		String[] link = a.split("url=");
		System.out.println(link[1]);
		String result = java.net.URLDecoder.decode(link[1], "UTF-8");
		System.out.println(result);
		}else{
			System.out.println(a);
		}*/
		
		double targetRowCount = 500000;
	    int row = (int)(targetRowCount/300000);
	    for(int i =0;i<row+1;i++){
	    	System.out.println("Row "+i);
	    }
		
	}

}
