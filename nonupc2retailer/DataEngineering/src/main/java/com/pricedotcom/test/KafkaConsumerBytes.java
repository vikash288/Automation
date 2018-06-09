package com.pricedotcom.test;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.pricedotcom.amazons3.S3Operation;
import com.pricedotcom.aws.AWSConnect;
import com.pricedotcom.util.ProjectProperties;


public class KafkaConsumerBytes {

	public static void main(String[] args) throws IOException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "pricedotcom");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		//props.put("fetch.message.max.bytes", "52428800");
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		
		ProjectProperties prop = new ProjectProperties();

	    consumer.subscribe(Arrays.asList("testing"));
	    AWSConnect awsConnect = new AWSConnect();
	    AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
	    S3Operation s3opp = new S3Operation();
	    
	    
		try {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(999999999);
				for (ConsumerRecord<String, byte[]> record : records){
			    
				if(record.key()!=null || record.key()!=""){	
				System.out.println("New file Ready to upload");
				InputStream bin = new ByteArrayInputStream(record.value());
				ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(record.value().length);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                String date = format.format(new Date());
                s3Client.putObject("pricingdotcom", date+"/walmart1/TJ_Maxx-TJ_Maxx_Product_Catalog.txt", bin, metadata);
                System.out.println("File has been successfully uploaded");
                bin.close();
                System.out.println("Data insertion statred");
                s3opp.readFromS3(s3Client, "pricingdotcom", date+"/walmart1/TJ_Maxx-TJ_Maxx_Product_Catalog.txt");
                
                }else{
                	System.out.println("Else part"+record.key());
                }
				
				}
			}
		} finally {
			consumer.close();
		}
	}
}

