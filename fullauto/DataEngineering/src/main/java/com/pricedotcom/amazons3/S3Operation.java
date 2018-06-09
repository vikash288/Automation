package com.pricedotcom.amazons3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.client.methods.HttpRequestBase;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.devicefarm.model.Upload;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.util.Md5Utils;
import com.pricedotcom.aws.AWSConnect;
import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;
import com.pricedotcom.util.ProjectProperties;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * @author abhinandan
 *
 */
public class S3Operation {

	/**
	 * @param s3client
	 * @param bucketName
	 */
	public Bucket createBucket(AmazonS3 s3client, String bucketName) {
		s3client.setRegion(Region.getRegion(Regions.US_WEST_2));
		Bucket bucket = null;
		if (!s3client.doesBucketExist(bucketName)) {
			bucket = s3client.createBucket(bucketName);
		}

		return bucket;
	}

	/**
	 * @param s3client
	 * @return
	 */
	public List<Bucket> listofBuckets(AmazonS3 s3client) {
		List<Bucket> buckets = s3client.listBuckets();
		return buckets;
	}

	/**
	 * @param bucketName
	 * @param folderName
	 * @param client
	 */
	public PutObjectResult createFolder(String bucketName, String folderName, AmazonS3 s3client) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + "/", emptyContent, metadata);
		// send request to S3 to create folder
		PutObjectResult result = s3client.putObject(putObjectRequest);
		return result;
	}

	/**
	 * @param s3client
	 * @param bucketName
	 */
	public void deleteBucket(AmazonS3 s3client, String bucketName) {
		s3client.deleteBucket(bucketName);
	}

	/**
	 * @param s3client
	 * @param bucketName
	 */
	public void listofObjects(AmazonS3 s3client, String bucketName) {
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix("2017-01-27/Newegg");
		ObjectListing objectListing=s3client.listObjects(listObjectsRequest);
		List<S3ObjectSummary> objectSummary = objectListing.getObjectSummaries();
		List<String> al = new ArrayList<String>();
		// add elements to al, including duplicates
		Set<String> hs = new HashSet<>();

		al.clear();
		al.addAll(hs);

		/*String a = "2017-01-27/Newegg";
		String aa[] = a.split("/");

		do {
			objectListing = s3client.listObjects(listObjectsRequest);
			System.out.println();
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				String key = objectSummary.getKey();
				String keyArray[] = key.split("/");
				if (key.contains("/")) {
					
			        	  al.add(keyArray[2]);
			         
					
				} else {
					al.add(keyArray[2]);
				}
				System.out.println(objectSummary.getKey());
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		hs.addAll(al);
		al.clear();
		al.addAll(hs);

		for (String s : hs) {
			System.out.println("Folder Name: " + s);
		}*/

	}

	/**
	 * @param existingBucketName
	 * @param keyName
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public String S3Upload(AmazonS3 s3Client, String existingBucketName, String keyName, String filePath)
			throws IOException {

		String eTag = "";

		// Create a list of UploadPartResponse objects. You get one of these
		// for each part upload.
		List<PartETag> partETags = new ArrayList<PartETag>();

		// Step 1: Initialize.
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(existingBucketName, keyName);
		InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

		File file = new File(filePath);
		long contentLength = file.length();
		long partSize = 5242880; // Set part size to 5 MB.

		try {
			// Step 2: Upload parts.
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				// Last part can be less than 5 MB. Adjust part size.
				partSize = Math.min(partSize, (contentLength - filePosition));

				// Create request to upload a part.
				UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(existingBucketName)
						.withKey(keyName).withUploadId(initResponse.getUploadId()).withPartNumber(i)
						.withFileOffset(filePosition).withFile(file).withPartSize(partSize);

				// Upload part and add response to our list.
				partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

				filePosition += partSize;
			}

			// Step 3: Complete.
			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(existingBucketName, keyName,
					initResponse.getUploadId(), partETags);

			CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(compRequest);
			eTag = result.getETag();
			// logger.info("File upload successfully in Amazon S3 with a bucket
			// name:" + existingBucketName
			// + " with a file name:" + keyName + " Etag value:" + eTag);
		} catch (Exception e) {
			// logger.error("Runtime exception occured:" + e.toString());
			s3Client.abortMultipartUpload(
					new AbortMultipartUploadRequest(existingBucketName, keyName, initResponse.getUploadId()));
		}

		return eTag;
	}

	public static String getFileMD5(String filePath) throws IOException {
		FileInputStream fis = new FileInputStream(new File(filePath));
		// String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
		// fis.close();
		byte[] resultByte = DigestUtils.md5(fis);
		String streamMD5 = new String(Base64.encodeBase64(resultByte));
		return streamMD5;
	}

	public void setConentMD5Hash(AmazonS3 s3Client, String bucketName, String key, String eTag) {

		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		s3object.getObjectMetadata().setContentMD5(eTag);

	}

	public String getContentMD5Hash(AmazonS3 s3Client, String bucketName, String key) {

		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		return s3object.getObjectMetadata().getContentMD5();
	}

	public Long getContentLength(AmazonS3 s3Client, String bucketName, String key) {

		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		return s3object.getObjectMetadata().getContentLength();

	}

	public void readFromS3(AmazonS3 s3Client, String bucketName, String key) throws IOException {
		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		System.out.println(s3object.getObjectMetadata().getContentType());
		System.out.println(s3object.getObjectMetadata().getContentLength());
		System.out.println(s3object.getObjectMetadata().getContentMD5());

		MysqlConnection mysqlconn = new MysqlConnection();
		Connection con = null;
		try {
			con = mysqlconn.connectMySQL();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			con.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		MySqlOperation mysqlopp = new MySqlOperation();
		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;

		// Truncate the table before load
		try {
			Statement st = con.createStatement();
			st.executeUpdate("TRUNCATE TJ_MAXX_PRODUCT_CATALOG");
			st.close();
			System.out.println("TRUNCATE SUCCESSFUL!");
		} catch (SQLException e) {
			e.printStackTrace();
		}

		line = reader.readLine();
		while ((line = reader.readLine()) != null) {

			//mysqlopp.processData(line, con);

		}

		try {
			con.close();
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	public void writeinS3(AmazonS3 s3Client, String bucketName, String key, InputStream objectContent)
			throws IOException {
		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		String source = "This is the source of my input stream";
		s3object.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(source.getBytes()), null));
	}

	public void moveBucketContain(AmazonS3 s3Client, String sourceBucket, String trashBucket) {
		ObjectListing listing = s3Client.listObjects(sourceBucket);
		List<S3ObjectSummary> objectSummary = listing.getObjectSummaries();
		for (int i = 0; i < objectSummary.size(); i++) {
			String key = objectSummary.get(i).getKey();
			System.out.println("Keys--> " + key);
			s3Client.copyObject(sourceBucket, key, trashBucket, key);
			s3Client.deleteObject(sourceBucket, key);
		}
	}

	public void moveObject(AmazonS3 s3Client, String sourceBucket, String key, String trashBucket) {
		s3Client.copyObject(sourceBucket, key, trashBucket, key);
		s3Client.deleteObject(sourceBucket, key);
	}

	/*
	 * public static void main(String args[]) throws IOException {
	 * ProjectProperties prop = new ProjectProperties(); AWSConnect awsConnect =
	 * new AWSConnect(); AmazonS3 s3Client =
	 * awsConnect.s3ClientConnect(prop.getAccesskey(), prop.getSecretkey());
	 * S3Operation s3 = new S3Operation(); List<Bucket> l =
	 * s3.listofBuckets(s3Client); for (int i = 0; i < l.size(); i++) {
	 * System.out.println(l.get(i).getName()); } s3.readFromS3(s3Client,
	 * "pricingdotcom",
	 * "2017-01-30/walmart1/TJ_Maxx-TJ_Maxx_Product_Catalog.txt"); }
	 */
}
