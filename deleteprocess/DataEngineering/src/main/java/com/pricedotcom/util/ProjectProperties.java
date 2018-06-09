package com.pricedotcom.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProjectProperties {
	

	private static String accessKey="accessKey";
	private static String secretKey="secretKey";
	private static String bucketName="bucketName";
	private static String dbclassname="dbclassname";
	private static String jdbcurl="jdbcurl";
	private static String dbusername="dbusername";
	private static String dbpassword="dbpassword";
	private static String dbtarget="dbtaget";
	private static String dbstg="dbstg";
	private static String dumpfilepath="dumpfilepath";
	private static String eslink="eslink";
	private static String esindex="esindex";
	private static String eslinkauto="eslinkauto";
	private static String esindexauto="esindexauto";
	private static String usedeslink="usedeslink";
	private static String usedesindex="usedesindex";
	private static String logpath="logpath";
	
	
	static Properties prop = new Properties();
	static InputStream input = Thread.currentThread().getContextClassLoader()
           .getResourceAsStream("config.properties");
   
	/**
	 * @return the accesskey
	 * @throws IOException 
	 */
	public String getAccesskey() throws IOException {
		prop.load(input);
		return prop.getProperty(accessKey);
	}
	/**
	 * @return the secretkey
	 * @throws IOException 
	 */
	public String getSecretkey() throws IOException {
		prop.load(input);
		return prop.getProperty(secretKey);
	}
	
	/**
	 * @return the bucketName
	 * @throws IOException 
	 */
	public String getBucketName() throws IOException {
		prop.load(input);
		return prop.getProperty(bucketName);
	}
	
	/**
	 * @return the dbclassname
	 * @throws IOException 
	 */
	public static String getDbclassname() throws IOException {
		prop.load(input);
		return prop.getProperty(dbclassname);
	}
	/**
	 * @return the jdbcurl
	 * @throws IOException 
	 */
	public static String getJdbcurl() throws IOException {
		prop.load(input);
		return prop.getProperty(jdbcurl);
	}
	/**
	 * @return the dbusername
	 * @throws IOException 
	 */
	public static String getDbusername() throws IOException {
		prop.load(input);
		return prop.getProperty(dbusername);
	}
	/**
	 * @return the dbpassword
	 * @throws IOException 
	 */
	public static String getDbpassword() throws IOException {
		prop.load(input);
		return prop.getProperty(dbpassword);
	}
	/**
	 * @return the dumpfilepath
	 * @throws IOException 
	 */
	public static String getDumpfilepath() throws IOException {
		prop.load(input);
		//return "/home/ubuntu/DUMP"; 
		return prop.getProperty(dumpfilepath);
	}
	
	/**
	 * @return the eslink
	 * @throws IOException 
	 */
	public static String getEslink() throws IOException {
		prop.load(input);
		return prop.getProperty(eslink);
	}
	/**
	 * @return the esindex
	 * @throws IOException 
	 */
	public static String getEsindex() throws IOException {
		prop.load(input);
		return prop.getProperty(esindex);
	}
		
	/**
	 * @return the usedeslink
	 * @throws IOException 
	 */
	public static String getUsedeslink() throws IOException {
		prop.load(input);
		return prop.getProperty(usedeslink);
	}
	/**
	 * @return the usedesindex
	 * @throws IOException 
	 */
	public static String getUsedesindex() throws IOException {
		prop.load(input);
		return prop.getProperty(usedesindex);
	}
	/**
	 * @param usedeslink the usedeslink to set
	 */
	public static void setUsedeslink(String usedeslink) {
		ProjectProperties.usedeslink = usedeslink;
	}
	/**
	 * @param usedesindex the usedesindex to set
	 */
	public static void setUsedesindex(String usedesindex) {
		ProjectProperties.usedesindex = usedesindex;
	}
	

	/**
	 * @return the dbtarget
	 * @throws IOException 
	 */
	public static String getDbtarget() throws IOException {
		prop.load(input);
		return prop.getProperty(dbtarget);
	}
	/**
	 * @return the dbstg
	 * @throws IOException 
	 */
	public static String getDbstg() throws IOException {
		prop.load(input);
		return prop.getProperty(dbstg);
	}
	/**
	 * @param dbtarget the dbtarget to set
	 */
	public static void setDbtarget(String dbtarget) {
		ProjectProperties.dbtarget = dbtarget;
	}
	/**
	 * @param dbstg the dbstg to set
	 */
	public static void setDbstg(String dbstg) {
		ProjectProperties.dbstg = dbstg;
	}
	/**
	 * @return the logpath
	 * @throws IOException 
	 */
	public static String getLogpath() throws IOException {
		prop.load(input);
		return prop.getProperty(logpath);
	}
	/**
	 * @param logpath the logpath to set
	 */
	public static void setLogpath(String logpath) {
		ProjectProperties.logpath = logpath;
	}

	/**
	 * @return the eslinkauto
	 * @throws IOException 
	 */
	public static String getEslinkauto() throws IOException {
		prop.load(input);
		return prop.getProperty(eslinkauto);
	}
	/**
	 * @return the esindexauto
	 * @throws IOException 
	 */
	public static String getEsindexauto() throws IOException {
		prop.load(input);
		return prop.getProperty(esindexauto);
	}
	/**
	 * @param eslinkauto the eslinkauto to set
	 */
	public static void setEslinkauto(String eslinkauto) {
		ProjectProperties.eslinkauto = eslinkauto;
	}
	/**
	 * @param esindexauto the esindexauto to set
	 */
	public static void setEsindexauto(String esindexauto) {
		ProjectProperties.esindexauto = esindexauto;
	}
	
}
