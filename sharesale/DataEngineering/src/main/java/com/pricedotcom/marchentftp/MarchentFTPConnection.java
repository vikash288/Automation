package com.pricedotcom.marchentftp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;
import org.joda.time.Interval;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.pricedotcom.amazons3.S3Operation;
import com.pricedotcom.aws.AWSConnect;
import com.pricedotcom.util.EmailUtil;
import com.pricedotcom.util.LogUtil;
import com.pricedotcom.util.ProjectProperties;

public class MarchentFTPConnection {
	Logger logger = Logger.getLogger(MarchentFTPConnection.class);
	LogUtil log = new LogUtil();
	ProjectProperties prop = new ProjectProperties();
	AWSConnect awsConnect = new AWSConnect();
	S3Operation s3Opp = new S3Operation();
	private final EmailUtil mail = new EmailUtil();
	private final LogUtil logutil = new LogUtil();

	public void httpDownloadFile(String fileURL, String retailer) throws IOException {
		String saveDir = prop.getDumpfilepath();
		int BUFFER_SIZE = 4096;
		URL url = new URL(fileURL);
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		int responseCode = httpConn.getResponseCode();

		// always check HTTP response code first
		if (responseCode == HttpURLConnection.HTTP_OK) {
			String fileName = "";
			String disposition = httpConn.getHeaderField("Content-Disposition");
			String contentType = httpConn.getContentType();
			int contentLength = httpConn.getContentLength();

			if (disposition != null) {
				// extracts file name from header field
				int index = disposition.indexOf("filename=");
				if (index > 0) {
					fileName = disposition.substring(index + 10, disposition.length() - 1);
				}
			} else {
				// extracts file name from URL
				fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1, fileURL.length());
			}

			System.out.println("Content-Type = " + contentType);
			System.out.println("Content-Disposition = " + disposition);
			System.out.println("Content-Length = " + contentLength);
			System.out.println("fileName = " + fileName);

			// opens input stream from the HTTP connection
			InputStream inputStream = httpConn.getInputStream();
			String saveFilePath = saveDir + File.separator + fileName;

			// opens an output stream to save into file
			FileOutputStream outputStream = new FileOutputStream(saveFilePath);

			int bytesRead = -1;
			byte[] buffer = new byte[BUFFER_SIZE];
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, bytesRead);
			}

			outputStream.close();
			inputStream.close();

			System.out.println("File downloaded");
			String outputFile = prop.getDumpfilepath() + "/" + fileName;
			File outputF = new File(outputFile);
			InputStream is = new FileInputStream(outputF);
			AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(outputF.length());
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String date = format.format(new Date());
			// sourceFile = sourceFile.substring(0,
			// sourceFile.lastIndexOf("."));
			s3Client.putObject(prop.getBucketName(), date + "/" + retailer + "/" + fileName, is, metadata);

			System.out.println("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
					+ " file location: " + date + "/" + retailer + "/" + fileName);

			outputF.delete();
			is.close();
		} else {
			System.out.println("No file to download. Server replied HTTP code: " + responseCode);
		}
		httpConn.disconnect();
	}

	
	
	
	public String httpDownloadFileS3(String fileURL, String retailer) throws IOException {
		String filePath="";
		String saveDir = prop.getDumpfilepath();
		int BUFFER_SIZE = 4096;
		URL url = new URL(fileURL);
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		int responseCode = httpConn.getResponseCode();

		// always check HTTP response code first
		if (responseCode == HttpURLConnection.HTTP_OK) {
			String fileName = "";
			String disposition = httpConn.getHeaderField("Content-Disposition");
			String contentType = httpConn.getContentType();
			int contentLength = httpConn.getContentLength();

			if (disposition != null) {
				// extracts file name from header field
				int index = disposition.indexOf("filename=");
				if (index > 0) {
					fileName = disposition.substring(index + 10, disposition.length() - 1);
				}
			} else {
				// extracts file name from URL
				fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1, fileURL.length());
			}

			System.out.println("Content-Type = " + contentType);
			System.out.println("Content-Disposition = " + disposition);
			System.out.println("Content-Length = " + contentLength);
			System.out.println("fileName = " + fileName);

			// opens input stream from the HTTP connection
			InputStream inputStream = httpConn.getInputStream();
			String saveFilePath = saveDir + File.separator + fileName;

			// opens an output stream to save into file
			FileOutputStream outputStream = new FileOutputStream(saveFilePath);

			int bytesRead = -1;
			byte[] buffer = new byte[BUFFER_SIZE];
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, bytesRead);
			}

			outputStream.close();
			inputStream.close();

			System.out.println("File downloaded");
			String outputFile = prop.getDumpfilepath() + "/" + fileName;
			File outputF = new File(outputFile);
			filePath=outputF.getAbsolutePath();
			InputStream is = new FileInputStream(outputF);
			AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(outputF.length());
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String date = format.format(new Date());
			// sourceFile = sourceFile.substring(0,
			// sourceFile.lastIndexOf("."));
			//s3Client.putObject(prop.getBucketName(), date + "/" + retailer + "/" + fileName, is, metadata);
			
			String etag = s3Opp.S3Upload(s3Client, prop.getBucketName(), date + "/" + retailer + "/" + fileName,
					outputFile);

			System.out.println("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
					+ " file location: " + date + "/" + retailer + "/" + fileName);

			outputF.delete();
			is.close();
		} else {
			System.out.println("No file to download. Server replied HTTP code: " + responseCode);
		}
		httpConn.disconnect();
		return filePath;
	}

	
	public String httpDownloadFile(String fileURL, String retailer, String fileLocation,String logpath) {
		logger.addAppender(log.getLogProp(logpath));
		String filePath = "";
		String saveDir = fileLocation;
		try {
			int BUFFER_SIZE = 4096;
			URL url;
			url = new URL(fileURL);
			logger.info("Download Link "+fileURL);
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			int responseCode = httpConn.getResponseCode();
			logger.info("Http Response Code:  "+responseCode);

			// always check HTTP response code first
			if (responseCode == HttpURLConnection.HTTP_OK) {
				String fileName = "";
				String disposition = httpConn.getHeaderField("Content-Disposition");
				String contentType = httpConn.getContentType();
				int contentLength = httpConn.getContentLength();
				
				

				if (disposition != null) {
					// extracts file name from header field
					int index = disposition.indexOf("filename=");
					if (index > 0) {
						fileName = disposition.substring(index + 10, disposition.length() - 1);
					}
				} else {
					// extracts file name from URL
					fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1, fileURL.length());
				}

				logger.info("Content-Type = " + contentType);
				logger.info("Content-Disposition = " + disposition);
				logger.info("Content-Length = " + contentLength);
				logger.info("fileName = " + fileName);
				
				System.out.println("Content-Type = " + contentType);
				System.out.println("Content-Disposition = " + disposition);
				System.out.println("Content-Length = " + contentLength);
				System.out.println("fileName = " + fileName);

				// opens input stream from the HTTP connection
				InputStream inputStream = httpConn.getInputStream();
				String saveFilePath = saveDir + File.separator + fileName;

				// opens an output stream to save into file
				FileOutputStream outputStream = new FileOutputStream(saveFilePath);

				int bytesRead = -1;
				byte[] buffer = new byte[BUFFER_SIZE];
				while ((bytesRead = inputStream.read(buffer)) != -1) {
					outputStream.write(buffer, 0, bytesRead);
				}

				outputStream.close();
				inputStream.close();
				filePath = saveDir + "/" + fileName;
				
				//AWS S3 Upload Process
				
				AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				String date = format.format(new Date());
				String sourceFile = fileName;
				System.out.println("File Upload Started");
				String etag = s3Opp.S3Upload(s3Client, prop.getBucketName(), date + "/" + retailer + "/" + sourceFile,
						filePath);
				System.out.println("File Download Complete , Etag value: " + etag);
				if (!etag.isEmpty()) {

					logger.info("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
							+ " file location: " + date + "/" + retailer + "/" + sourceFile + " Etag Value: " + etag);
				}else{
					filePath ="";
				}

				System.out.println("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
						+ " file location: " + date + "/" + retailer + "/" + sourceFile);

			} else {
				System.out.println("No file to download. Server replied HTTP code: " + responseCode);
				logger.warn("No file to download. Server replied HTTP code: " + responseCode);
			}
			httpConn.disconnect();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return filePath;
	}
	
	
	
	public String gunzipIt(InputStream input) throws IOException {
		String output = ProjectProperties.getDumpfilepath() + "/dump_" + new Date().getTime();
		byte[] buffer = new byte[1024];
		try {
			GZIPInputStream gzis = new GZIPInputStream(input);
			FileOutputStream out = new FileOutputStream(output);
			int len;
			while ((len = gzis.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}
			gzis.close();
			out.close();
			System.out.println("Done");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		return output;
	}

	public byte[] ftpConnection(String hostName, String userName, String password, int port, String sourceFolderName,
			String sourceFile, String retailer) {

		FTPClient ftpClient = new FTPClient();
		boolean result;
		byte[] data = null;
		// Connect to the localhost
		try {
			ftpClient.connect(hostName, port);
			// login to ftp server
			result = ftpClient.login(userName, password);
			ftpClient.enterLocalPassiveMode();

			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			if (result == true) {
				// logger.info("Successfully logged in into FTP Server! , Server
				// HostName: "+hostName);
				System.out.println("Successfully logged in into FTP Server! , Server HostName: " + hostName);
				String remoteFile = "/" + sourceFolderName + "/" + sourceFile;
				InputStream inputStream = ftpClient.retrieveFileStream(remoteFile);
				String output = this.gunzipIt(inputStream);
				File outputFile = new File(output);
				inputStream = new FileInputStream(outputFile);
				if (outputFile.length() > (200 * 1024 * 1024)) {
					System.out.println("File size grater than 200 MB , so Directely load into S3 ");
					AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
					ObjectMetadata metadata = new ObjectMetadata();
					metadata.setContentLength(outputFile.length());
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					String date = format.format(new Date());
					sourceFile = sourceFile.substring(0, sourceFile.lastIndexOf("."));
					s3Client.putObject(prop.getBucketName(), date + "/" + retailer + "/" + sourceFile, inputStream,
							metadata);
					System.out.println("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
							+ " file location: " + date + "/" + retailer + "/" + sourceFile);

					inputStream.close();
					outputFile.delete();
				} else {
					ByteArrayOutputStream bao = new ByteArrayOutputStream();
					byte[] bytesArray = new byte[4048];
					int bytesRead = -1;
					while ((bytesRead = inputStream.read(bytesArray)) != -1) {
						bao.write(bytesArray, 0, bytesRead);
					}

					data = bao.toByteArray();
					inputStream.close();
					bao.close();
					outputFile.delete();
				}
			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// logger.info("Socket Exception "+e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// logger.info("IOException "+e.toString());
		}
		return data;

	}

	public String ftpDirectUpload(String hostName, String userName, String password, int port, String sourceFolderName,
			String sourceFile, String retailer) {

		String fileName = "";
		FTPClient ftpClient = new FTPClient();
		boolean result;
		// Connect to the localhost
		try {
			ftpClient.connect(hostName, port);
			// login to ftp server
			result = ftpClient.login(userName, password);
			ftpClient.enterLocalPassiveMode();

			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			if (result == true) {
				// logger.info("Successfully logged in into FTP Server! , Server
				// HostName: "+hostName);

				System.out.println("Successfully logged in into FTP Server! , Server HostName: " + hostName);
				String remoteFile = "/" + sourceFolderName + "/" + sourceFile;
				System.out.println("Remote File Location: " + remoteFile);
				InputStream inputStream = ftpClient.retrieveFileStream(remoteFile);
				String output = this.gunzipIt(inputStream);
				// File outputFile = new File(output);
				// inputStream = new FileInputStream(outputFile);

				AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
				// ObjectMetadata metadata = new ObjectMetadata();
				// metadata.setContentLength(outputFile.length());
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				String date = format.format(new Date());
				sourceFile = sourceFile.substring(0, sourceFile.lastIndexOf("."));
				String etag = s3Opp.S3Upload(s3Client, prop.getBucketName(), date + "/" + retailer + "/" + sourceFile,
						output);
				if (!etag.isEmpty()) {
					fileName = output;
				}

				System.out.println("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
						+ " file location: " + date + "/" + retailer + "/" + sourceFile);

				/*
				 * s3Client.putObject(prop.getBucketName(), date + "/" +
				 * retailer + "/" + sourceFile, inputStream, metadata);
				 * 
				 * inputStream.close(); outputFile.delete();
				 */

			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// logger.info("Socket Exception "+e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// logger.info("IOException "+e.toString());
		}

		return fileName;
	}

	public int ftpFileModificationDate(String hostName, String userName, String password, int port,
			String sourceFolderName, String sourceFile, String retailer, String logpath) {

		logger.addAppender(log.getLogProp(logpath));

		FTPClient ftpClient = new FTPClient();
		boolean result;
		int diffInDays = 0;
		System.out.println("Modified Date");
		// Connect to the localhost
		try {
			ftpClient.connect(hostName, port);
			// login to ftp server
			result = ftpClient.login(userName, password);
			ftpClient.enterLocalPassiveMode();

			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			if (result == true) {
				System.out.println("Successfully logged in into FTP Server! , Server HostName: " + hostName);
				logger.info("Successfully logged in into FTP Server! , Server HostName: " + hostName);
				String remoteFile = "/" + sourceFolderName + "/" + sourceFile;
				String time = ftpClient.getModificationTime(remoteFile);
				System.out.println(time);
				Date modDate = this.printTime(time);
				diffInDays = (int) ((new Date().getTime() - modDate.getTime()) / (1000 * 60 * 60 * 24));

			}

			ftpClient.logout();
		} catch (Exception e) {

		}
		return diffInDays;

	}

	public String ftpDirectUpload(String hostName, String userName, String password, int port, String sourceFolderName,
			String sourceFile, String retailer, String affiliator, String logpath) {

		logger.addAppender(log.getLogProp(logpath));

		String fileName = "";
		FTPClient ftpClient = new FTPClient();
		boolean result;
		// Connect to the localhost
		try {
			ftpClient.connect(hostName, port);
			// login to ftp server
			result = ftpClient.login(userName, password);
			ftpClient.enterLocalPassiveMode();

			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			if (result == true) {
				System.out.println("Successfully logged in into FTP Server! , Server HostName: " + hostName);
				logger.info("Successfully logged in into FTP Server! , Server HostName: " + hostName);
				String remoteFile = "/" + sourceFolderName + "/" + sourceFile;
				System.out.println("Remote File Location: " + remoteFile);
				logger.info("Remote File Location: " + remoteFile);
				String time = ftpClient.getModificationTime(remoteFile);
				// Date modificationDate = this.printTime(time);

				/*
				 * if(!this.checkFileExists(hostName, userName, password, port,
				 * remoteFile)){ logger.error("File Not Found in FTP server");
				 * String mailBody = logutil.readLogFile(retailer,
				 * ProjectProperties.getLogpath() + logpath);
				 * mail.sendMailGet(affiliator.toUpperCase() + ":" + retailer +
				 * " Load Failed ",
				 * "Hello, \n Data Load Process failed, File not Found In FTP Server , \n "
				 * +mailBody);
				 * 
				 * }
				 */

				InputStream inputStream = ftpClient.retrieveFileStream(remoteFile);
				int returnCode = ftpClient.getReplyCode();

				if (inputStream == null || returnCode == 550) {
					System.out.println("File Not Found in FTP server");
					logger.error("File Not Found in FTP server");
					String mailBody = logutil.readLogFile(retailer, ProjectProperties.getLogpath() + logpath);
					mail.sendMailGet(affiliator.toUpperCase() + ":" + retailer + " Load Failed ",
							"Hello, \n Data Load Process failed, File not Found In FTP Server , \n " + mailBody);
				}

				String output = this.gunzipIt(inputStream);
				inputStream.close();
				AmazonS3 s3Client = awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				String date = format.format(new Date());
				sourceFile = sourceFile.substring(0, sourceFile.lastIndexOf("."));
				System.out.println("File Upload Started");
				String etag = s3Opp.S3Upload(s3Client, prop.getBucketName(), date + "/" + retailer + "/" + sourceFile,
						output);
				System.out.println("File Download Complete , Etag value: " + etag);
				if (!etag.isEmpty()) {
					fileName = output;

					logger.info("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
							+ " file location: " + date + "/" + retailer + "/" + sourceFile + " Etag Value: " + etag);
				}

				System.out.println("File has been successfully uploaded on Bucket Name: " + prop.getBucketName()
						+ " file location: " + date + "/" + retailer + "/" + sourceFile);

				ftpClient.logout();

			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info("Socket Exception " + e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info("IOException " + e.toString());
		}

		return fileName;
	}

	public Date printTime(String time) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Date modificationTime = null;
		try {
			String timePart = time.split(" ")[1];
			modificationTime = dateFormat.parse(timePart);
			System.out.println("File modification time: " + modificationTime);
		} catch (ParseException ex) {
			ex.printStackTrace();
		}
		return modificationTime;
	}

	public boolean checkFileExists(String filePath, FTPClient ftpClient) throws IOException {
		InputStream inputStream = ftpClient.retrieveFileStream(filePath);
		int returnCode = ftpClient.getReplyCode();
		if (inputStream == null || returnCode == 550) {
			return false;
		}
		return true;
	}

	public boolean checkFileExists(String hostName, String userName, String password, int port, String filePath)
			throws IOException {

		FTPClient ftpClient = new FTPClient();

		System.out.println("Check file Exists");
		// Connect to the localhost

		ftpClient.connect(hostName, port);
		// login to ftp server
		ftpClient.login(userName, password);
		ftpClient.enterLocalPassiveMode();

		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

		InputStream inputStream = ftpClient.retrieveFileStream(filePath);
		int returnCode = ftpClient.getReplyCode();
		if (inputStream == null || returnCode == 550) {
			return false;
		}
		return true;

	}

	public static void main(String args[]) throws IOException {
		 MarchentFTPConnection ftp = new MarchentFTPConnection();
		// ftp.httpDownloadFile("https://s3.amazonaws.com/price.com-affiliatefeed/Tumi/Tumi+products.csv",
		// "Tumi");

		 String hostName = "products.impactradius.com";
			String userName = "ps-ftp_11061";
			String password = "qJaJLi6L26";
			int port = 21;
			String sourceFolderName = "/UGG";
			String sourceFile = "UGG-Australia_IR.txt.gz";
			String retailer = "UGG";
			String affiliator = "impactradius";
			String delimiter = " ";
			String retailerno = "148";
			
			ftp.ftpDirectUpload(hostName, userName, password, port, sourceFolderName, sourceFile, retailer, affiliator, "");
		
		
		 
		 ////String a = ftp.httpDownloadFile("https://s3.amazonaws.com/price.com-affiliatefeed/Marmot/Marmot+products.csv", "papperjam", "/home/abhinandan/DUMP");
		 //System.out.println("File Path: "+a);
		/*
		 * String hostName = "datatransfer.cj.com"; String userName = "4708603";
		 * String password = "oRvGWar%"; int port = 21; String sourceFolderName
		 * = "/outgoing/productcatalog/194608/"; String sourceFile =
		 * "PETCO_Animal_Supplies-Product_Catalog.txt.gz"; String retailer =
		 * "Petco"; String affiliator = "cj"; String delimiter = " "; String
		 * retailerno = "6";
		 */

		/*
		 * MarchentFTPConnection ftp = new MarchentFTPConnection();
		 * 
		 * int a= ftp.ftpFileModificationDate(hostName, userName, password,
		 * port, sourceFolderName, sourceFile, retailer, "");
		 * 
		 * // ftp.printTime("213 20170712115739.000");
		 * 
		 * System.out.println("date diff is: "+a);
		 */

	}

}
