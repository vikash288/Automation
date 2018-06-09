package com.pricedotcom.kafka;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.pricedotcom.marchentftp.MarchentFTPConnection;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MarchentDataIngestion {

	// final static Logger logger =
	// Logger.getLogger(MarchentDataIngestion.class);

	public void dataIngestion(String ftpHostname, String ftpUsername, String ftpPassword, int port,
			String sourceFolderName, String sourceFile, String topic, String retailer) {
		// logger.info("Kafka Producer Initiated");
		System.out.println("Kafka Producer Initiated");
		MarchentFTPConnection ftpcon = new MarchentFTPConnection();

		Properties props = new Properties();

		props.put("metadata.broker.list", "localhost:9092");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

		byte[] byteData = ftpcon.ftpConnection(ftpHostname, ftpUsername, ftpPassword, port, sourceFolderName,
				sourceFile, retailer);
		if (byteData.length > 0) {
			sourceFile = sourceFile.substring(0, sourceFile.lastIndexOf("."));
			KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic,
					String.valueOf(retailer + "/" + sourceFile + "_#_" + System.currentTimeMillis()), byteData);
			producer.send(data);
			// logger.info("File Send into topic- "+topic+" from Producer Source
			// information FTP Host Name: "+ftpHostname+" , Source Folder:
			// "+sourceFolderName+", Source File: "+sourceFile);
			System.out.println("File Send into topic- " + topic + " from Producer Source information FTP Host Name: "
					+ ftpHostname + " , Source Folder: " + sourceFolderName + ", Source File: " + sourceFile);
			producer.close();
		}

	}

}
