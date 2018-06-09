package com.pricedotcom.test;

import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.pricedotcom.marchentftp.MarchentFTPConnection;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducerBytes {
	
	public static void main(String[] args) throws InterruptedException {
		
		MarchentFTPConnection ftpcon = new  MarchentFTPConnection();
		 
		  Properties props = new Properties();
		 
		  props.put("metadata.broker.list", "localhost:9092");
		  props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		  props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		  props.put("request.required.acks", "1");
		  //props.put("producer.type","async");
		 // props.put("max.request.size", "52428800");
		  ProducerConfig config = new ProducerConfig(props);
		 
		  Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
		  
		  
		  
		  String topic = "testing";
		  byte[] byteData = ftpcon.ftpConnection("datatransfer.cj.com", "4708603", "oRvGWar%", 21,"outgoing/productcatalog/186334","TJ_Maxx-TJ_Maxx_Product_Catalog.txt.gz","");
		  System.out.println(byteData.length);
		  KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, String.valueOf(System.currentTimeMillis()), byteData);
		  producer.send(data);
		  
		  producer.close();
		  
		 }

}
