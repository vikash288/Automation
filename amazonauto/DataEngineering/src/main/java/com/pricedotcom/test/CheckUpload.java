package com.pricedotcom.test;

import java.io.IOException;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.pricedotcom.amazons3.S3Operation;
import com.pricedotcom.aws.AWSConnect;
import com.pricedotcom.util.ProjectProperties;

public class CheckUpload {
	
	public static void main(String args[]) throws IOException {
		ProjectProperties prop = new ProjectProperties();
		AWSConnect awsConnect = new AWSConnect();
		//AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
		AmazonS3 s3Client = awsConnect.s3ClientConnect("AKIAISEDXKBXS77YYJIQ", "msS5RL0hvnUznFko2ZI9M2MS06Z8H6fSgY+K0Olb");
		S3Operation s3 = new S3Operation();
		/*List<Bucket> l = s3.listofBuckets(s3Client);
		for (int i = 0; i < l.size(); i++) {
			System.out.println(l.get(i).getName());
		}*/
		//s3.readFromS3(s3Client, "pricingdotcom", "2017-01-30/walmart1/TJ_Maxx-TJ_Maxx_Product_Catalog.txt");
		s3.listofObjects(s3Client, "pricingdotcom");
		
	}

}
