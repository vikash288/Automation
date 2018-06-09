package com.pricedotcom.kafka;

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

public class MarchentDataConsumer {

	//final static Logger logger = Logger.getLogger(MarchentDataConsumer.class);

	public void dataConsumer(String topic) throws IOException {
		//logger.info("Kafka Consumer Initiated");
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "pricedotcom");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

		ProjectProperties prop = new ProjectProperties();

		consumer.subscribe(Arrays.asList(topic));
		AWSConnect awsConnect = new AWSConnect();
		S3Operation s3opp = new S3Operation();

		try {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(999999);
				for (ConsumerRecord<String, byte[]> record : records) {

					if (record.key() != null || record.key() != "") {
						AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
						//logger.info("Amazon S3 Connected");
						System.out.println("Amazon S3 Connected");
						//logger.info("File Ready to upload, FileName: "+fileName);
						String keyname = record.key();
						String[] keysplit= keyname.split("_#_");
						System.out.println("File Ready to upload, FileName: "+keysplit[0]);
						InputStream bin = new ByteArrayInputStream(record.value());
						ObjectMetadata metadata = new ObjectMetadata();
						metadata.setContentLength(record.value().length);
						SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
						String date = format.format(new Date());
						s3Client.putObject(prop.getBucketName(), date + "/" + keysplit[0], bin,
								metadata);
						//logger.info("File has been successfully uploaded on Bucket Name: "+prop.getBucketName()+" file location: "+date + "/" + sourceName + "/" + fileName);
						System.out.println("File has been successfully uploaded on Bucket Name: "+prop.getBucketName()+" file location: "+date + "/" + keysplit[0]);
						bin.close();
						//s3opp.readFromS3(s3Client, prop.getBucketName(), date + "/" + keysplit[0]);
					} 

				}
			}
		} finally {
			consumer.close();
		}
	}

}
