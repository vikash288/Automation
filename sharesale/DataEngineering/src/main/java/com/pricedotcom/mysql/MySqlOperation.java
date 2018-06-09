package com.pricedotcom.mysql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pricedotcom.elasticsearch.ESOperations;
import com.pricedotcom.fileoperation.Fileoperation;
import com.pricedotcom.pojo.DQMPrice;
import com.pricedotcom.util.LogUtil;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author abhinandan
 *
 */
public class MySqlOperation {

	private Fileoperation fileOpp = new Fileoperation();
	ClassLoader classLoader = getClass().getClassLoader();
	LogUtil log = new LogUtil();
	Logger logger = Logger.getLogger(MySqlOperation.class);
	
	private ESOperations esopp = new ESOperations();

	/**
	 * @param conn
	 * @param stagingTablename
	 * @param delimiter
	 * @param path
	 * @return
	 */
	public long loadStagingData(Connection conn, String affilator, String stagingTablename, String delimiter,
			String path) {
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String truncateTable = "TRUNCATE TABLE " + stagingTablename;
			String fileLoad = "";
			if (affilator.equalsIgnoreCase("linkshare")) {
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " fields terminated by '" + delimiter + "' SET FIELD_39 = NOW();";
			}else if(affilator.equalsIgnoreCase("cj")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " SET FIELD_41 = NOW();";
			}

			stmt.execute(truncateTable);
			stmt.execute(fileLoad);

			fileOpp.deleteFile(path);

			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	/**
	 * @param conn
	 * @param stagingTablename
	 * @param delimiter
	 * @param path
	 * @return
	 */
	public long loadStagingData(Connection conn, String affilator, String stagingTablename, String delimiter,
			String path, String logpath) {
		logger.addAppender(log.getLogProp(logpath));
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String truncateTable = "TRUNCATE TABLE " + stagingTablename;
			String fileLoad = "";
			if (affilator.equalsIgnoreCase("linkshare")) {
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " fields terminated by '" + delimiter + "' SET FIELD_39 = NOW();";
			}else if(affilator.equalsIgnoreCase("cj")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " SET FIELD_41 = NOW();";
			}else if(affilator.equalsIgnoreCase("papperjam")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " fields terminated by '" + delimiter + "' ENCLOSED BY '\"' ESCAPED BY '\"' SET FIELD_89 = NOW();";
			}else if(affilator.equalsIgnoreCase("shareasale")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " fields terminated by '" + delimiter + "' ENCLOSED BY '\"' ESCAPED BY '\"' SET FIELD_52 = NOW();";
			}else if(affilator.equalsIgnoreCase("impactradius")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " SET FIELD_58 = NOW();";
			}
			
			stmt.execute(truncateTable);
			stmt.execute(fileLoad);
            logger.info("Staging table: "+stagingTablename+" truncated");
            logger.info("load data into Staging table: "+stagingTablename);
			fileOpp.deleteFile(path);

			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("SQL Exception "+e.toString());
		}
		return rowcnt;
	}

	/**
	 * @param conn
	 * @param stagingTablename
	 * @param delimiter
	 * @param path
	 * @return
	 */
	public long loadStagingData(Connection conn, String affiliator, String stagingTablename, String delimiter,
			List<String> path) {
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String truncateTable = "TRUNCATE TABLE " + stagingTablename;
			String fileLoad = "";
			stmt.execute(truncateTable);
			for (int i = 0; i < path.size(); i++) {
				if (affiliator.equalsIgnoreCase("linkshare")) {
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " fields terminated by '" + delimiter + "' SET FIELD_39 = NOW();";
				}else if(affiliator.equalsIgnoreCase("cj")){
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " SET FIELD_41 = NOW();";
				}
				stmt.execute(fileLoad);
				fileOpp.deleteFile(path.get(i));
			}

			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;

	}
	
	
	/**
	 * @param conn
	 * @param stagingTablename
	 * @param delimiter
	 * @param path
	 * @return
	 */
	public long loadStagingData(Connection conn, String affiliator, String stagingTablename, String delimiter,
			List<String> path, String logpath) {
		logger.addAppender(log.getLogProp(logpath));
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String truncateTable = "TRUNCATE TABLE " + stagingTablename;
			String fileLoad = "";
			stmt.execute(truncateTable);
			logger.info("Staging table: "+stagingTablename+" truncated");
			
			for (int i = 0; i < path.size(); i++) {
				if (affiliator.equalsIgnoreCase("linkshare")) {
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " fields terminated by '" + delimiter + "' SET FIELD_39 = NOW();";
				}else if(affiliator.equalsIgnoreCase("cj")){
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " SET FIELD_41 = NOW();";
				}else if(affiliator.equalsIgnoreCase("papperjam")){
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " fields terminated by '" + delimiter + "' ENCLOSED BY '\"' ESCAPED BY '\"' SET FIELD_89 = NOW();";
				}else if(affiliator.equalsIgnoreCase("shareasale")){
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " fields terminated by '" + delimiter + "' SET FIELD_52 = NOW();";
				}else if(affiliator.equalsIgnoreCase("impactradius")){
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " SET FIELD_58 = NOW();";
				}
				stmt.execute(fileLoad);
				fileOpp.deleteFile(path.get(i));
			}
			
            logger.info("load data into Staging table: "+stagingTablename);
			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("SQL Exception "+e.toString());
		}
		return rowcnt;

	}

	
	/**
	 * @param conn
	 * @param stagingTablename
	 * @param delimiter
	 * @param path
	 * @return
	 */
	public long loadIncrementalData(Connection conn, String affilator, String stagingTablename, String delimiter,
			String path) {
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String fileLoad = "";
			if (affilator.equalsIgnoreCase("linkshare")) {
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " fields terminated by '" + delimiter + "' SET FIELD_39 = NOW();";
			}else if(affilator.equalsIgnoreCase("cj")){
				fileLoad = "load data local infile '" + path + "' into table " + stagingTablename
						+ " SET FIELD_41 = NOW();";
			}

			stmt.execute(fileLoad);

			fileOpp.deleteFile(path);

			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	
	/**
	 * @param conn
	 * @param stagingTablename
	 * @param delimiter
	 * @param path
	 * @return
	 */
	public long loadIncrementalData(Connection conn, String affiliator, String stagingTablename, String delimiter,
			List<String> path) {
		long rowcnt = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			String rowCount = "select count(*) from " + stagingTablename;
			String fileLoad = "";
			for (int i = 0; i < path.size(); i++) {
				if (affiliator.equalsIgnoreCase("linkshare")) {
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " fields terminated by '" + delimiter + "' SET FIELD_39 = NOW();";
				}else if(affiliator.equalsIgnoreCase("cj")){
					fileLoad = "load data local infile '" + path.get(i) + "' into table " + stagingTablename
							+ " SET FIELD_41 = NOW();";
				}
				stmt.execute(fileLoad);
				fileOpp.deleteFile(path.get(i));
			}

			rs = stmt.executeQuery(rowCount);
			rs.next();
			rowcnt = Long.parseLong(rs.getString(1));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;

	}

	/**
	 * @param conn
	 * @param DBname
	 * @return
	 */
	public List<String> getAllTables(Connection conn, String DBname) {
		List<String> tableNames = new ArrayList<String>();
		try {
			Statement stmt = conn.createStatement();
			String showTables = "show tables in " + DBname;
			ResultSet rs = stmt.executeQuery(showTables);
			while (rs.next()) {
				tableNames.add(DBname+"."+rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tableNames;

	}

	/**
	 * @param conn
	 * @return
	 */
	public List<String> getAllTables(Connection conn) {
		List<String> tableNames = new ArrayList<String>();
		try {
			Statement stmt = conn.createStatement();
			String showTables = "show tables";
			ResultSet rs = stmt.executeQuery(showTables);
			while (rs.next()) {
				tableNames.add(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tableNames;
	}

	/**
	 * @param conn
	 * @param tableName
	 * @param affiliator
	 * @return
	 */
	public boolean createTable(Connection conn, String tableName, String affiliator) {
		boolean status = false;
		try {
			Statement stmt = conn.createStatement();
			String createTable = "";
			if (affiliator.equalsIgnoreCase("Linkshare")) {
				createTable = "create table " + tableName + " like LINKSHARE_TEMPLATE";
			}else if(affiliator.equalsIgnoreCase("cj")){
				createTable = "create table " + tableName + " like CJ_TEMPLATE";
			}else if(affiliator.equalsIgnoreCase("papperjam")){
				createTable = "create table " + tableName + " like PEPPERJAM_TEMPLATE";
			}else if(affiliator.equalsIgnoreCase("shareasale")){
				createTable = "create table " + tableName + " like SHAREASALE_TEMPLATE";
			}else if(affiliator.equalsIgnoreCase("impactradius")){
				createTable = "create table " + tableName + " like IMPACTRADIUS_TEMPLATE";
			}
			status = stmt.execute(createTable);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return status;

	}

	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long linkshareTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert) {
		long rowcnt = 0L;
		try {
			
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/linksharesql.csv")), ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("create table", "insert into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);

				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}
	
	
	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long linkshareTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert, String logpath) {
		logger.addAppender(log.getLogProp(logpath));
		
		long rowcnt = 0L;
		try {
			logger.info("Target insert/create process Started for Linkshare, target table name: "+targetTableName+" staging table name: "+stagingTableName);
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/linksharesql.csv")), ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("create table", "insert into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);
				logger.info("Target table Insert/Create process done");

				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("File Not found exception: "+e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("IOException: "+e.toString());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("SQLException: "+e.toString());
		}
		return rowcnt;
	}

	
	
	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long cjTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert) {
		long rowcnt = 0L;
		try {
	
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/cjsql.csv")), ',', '"', 1);
			
			//CSVReader reader = new CSVReader(new FileReader("sql/targetsql/cjsql.csv") , ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("create table", "insert into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);

				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	
	
	
	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long cjTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert, String logfile) {
		logger.addAppender(log.getLogProp(logfile));
		long rowcnt = 0L;
		try {
			logger.info("Target insert/create process Started for CJ, target table name: "+targetTableName+" staging table name: "+stagingTableName);
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/cjsql.csv")), ',', '"', 1);
			
			//CSVReader reader = new CSVReader(new FileReader("sql/targetsql/cjsql.csv") , ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("create table", "insert into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);
				logger.info("Target table Insert/Create process done");
				
				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	
	
	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long papperjamTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert, String logfile) {
		logger.addAppender(log.getLogProp(logfile));
		long rowcnt = 0L;
		try {
			logger.info("Target insert/create process Started for Papperjam, target table name: "+targetTableName+" staging table name: "+stagingTableName);
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/papperjamsql.csv")), ',', '"', 1);
			
			//CSVReader reader = new CSVReader(new FileReader("sql/targetsql/cjsql.csv") , ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("create table", "insert into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);
				logger.info("Target table Insert/Create process done");
				
				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	
	
	
	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long shareasaleTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert, String logfile) {
		logger.addAppender(log.getLogProp(logfile));
		long rowcnt = 0L;
		try {
			logger.info("Target insert/create process Started for Shareasale, target table name: "+targetTableName+" staging table name: "+stagingTableName);
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/shareshell.csv")), ',', '"', 1);
			
			//CSVReader reader = new CSVReader(new FileReader("sql/targetsql/cjsql.csv") , ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("ignore", "").replace("create table", "insert ignore into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);
				logger.info("Target table Insert/Create process done");
				
				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	
	
	
	/**
	 * @param conn
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long impactrediusTargetTableCreation(Connection conn, String stagingTableName, String targetTableName,
			String retailorNo, boolean insert, String logfile) {
		logger.addAppender(log.getLogProp(logfile));
		long rowcnt = 0L;
		try {
			logger.info("Target insert/create process Started for Impact Redius, target table name: "+targetTableName+" staging table name: "+stagingTableName);
			CSVReader reader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
			           .getResourceAsStream("sql/targetsql/implusredius.csv")), ',', '"', 1);
			
			//CSVReader reader = new CSVReader(new FileReader("sql/targetsql/cjsql.csv") , ',', '"', 1);
			// Read all rows at once
			List<String[]> allRows = reader.readAll();
			reader.close();
			for (String[] row : allRows) {

				Statement stmt = conn.createStatement();

				String step1 = row[1].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step2 = row[2];
				String step3 = row[3].replace("$Y", retailorNo).replace("$X", stagingTableName);
				String step4 = row[4];
				String step5 = "";
				String step6 = "select count(*) from " + targetTableName;
				if (insert) {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName)
							.replace("$Z", targetTableName).replace("create table", "insert into");
				} else {
					step5 = row[5].replace("$Y", retailorNo).replace("$X", stagingTableName).replace("$Z",
							targetTableName);
				}

				System.out.println("Step1: " + step1);
				stmt.execute(step1);
				// conn.commit();
				System.out.println("Step2: " + step2);
				stmt.execute(step2);
				// conn.commit();
				System.out.println("Step3: " + step3);
				stmt.execute(step3);
				// conn.commit();
				System.out.println("Step4: " + step4);
				stmt.execute(step4);
				// conn.commit();
				if (insert) {
					stmt.execute("truncate " + targetTableName);
					// conn.commit();
				}
				System.out.println("Step5: " + step5);
				stmt.execute(step5);
				logger.info("Target table Insert/Create process done");
				
				ResultSet rs = stmt.executeQuery(step6);

				rs.next();
				rowcnt = Long.parseLong(rs.getString(1));
				// conn.commit();
				// stmt.executeBatch();

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowcnt;
	}

	
	
	/**
	 * @param conn
	 * @param affiliatorName
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long targetTableCreation(Connection conn, String affiliatorName, String stagingTableName,
			String targetTableName, String retailorNo, boolean insert) {
		long rowcnt = 0;
		if (affiliatorName.equalsIgnoreCase("linkshare")) {
			rowcnt = this.linkshareTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert);
		}else if(affiliatorName.equalsIgnoreCase("cj")){
			rowcnt = this.cjTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert);
		}
		return rowcnt;
	}
	
	
	/**
	 * @param conn
	 * @param targetTableName
	 */
	public List<DQMPrice> dqmPriceCheck(Connection conn, String targetTableName) {
		List<DQMPrice> price = new ArrayList<DQMPrice>();
		try {
			String query = "select sum(price1) as sumprice1,sum(price2) as sumprice2 from "+targetTableName;
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				double price1 = rs.getDouble("sumprice1");
				double price2 = rs.getDouble("sumprice2");
				DQMPrice priceData = new DQMPrice(price1, price2);
				price.add(priceData);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return price;
	}
	
	
	/**
	 * @param conn
	 * @param tableName
	 * @param field
	 * @return
	 */
	public boolean updatepriceDQM(Connection conn, String tableName,String field){
		boolean flag = false;
		try {
			Statement stmt = conn.createStatement();
			String sql="";
			if(field.equalsIgnoreCase("price1")){
				sql = "update "+tableName+" set price1=price2";
				flag=stmt.execute(sql);
			}else if(field.equalsIgnoreCase("price2")){
				sql = "update "+tableName+" set price2=price1";
				flag=stmt.execute(sql);
			}else{
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
		
	}
	
	
	//SELECT SRC.* FROM "+target_previous_table_name+" SRC LEFT OUTER JOIN "+target_table_name+" TGT ON SRC.UPC = TGT.UPC WHERE TGT.UPC IS NULL
	
	// Delete partial record from ES
/*	public void deletePartialRecord(Connection conn, String targetTableName,String retailerNo, String retailerName,String logpath){
		logger.addAppender(log.getLogProp(logpath));
		logger.info("Offer delete started for "+retailerName+ " retailer no "+retailerNo);
		try {
			String query = "select MAPTABLE.UPC from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo);
			Statement stmt = conn.createStatement();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				esopp.partialDeleteCurl(rs.getString("UPC"), retailerName);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/	
	
	public void deletePartialRecord(Connection conn, String targetTableName,String retailerNo, String retailerName,String logpath){
		logger.addAppender(log.getLogProp(logpath));
		logger.info("Offer delete started for "+retailerName+ " retailer no "+retailerNo);
		try {
			//old select technique
		//	String query = "select MAPTABLE.UPC from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo);
			
			String query="select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC,"
					+ " MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE"
					+ " LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL"
					+ " and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo)+") as a "
					+ " where not exists  (Select common_id, upc, retailer_code, item_id from"
					+ " PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc and a.retailer_code = "+Integer.parseInt(retailerNo)+""
					+ " and DATE_FORMAT(b.AUDITTIME, '%Y/%m/%d')=DATE_FORMAT(now(), '%Y/%m/%d'))";

			String insertquery="insert into PRICE_DOT_COM_STAGING.OFFER_DELETE select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC,"
					+ " MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE"
					+ " LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL"
					+ " and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo)+") as a where not exists  (Select common_id, upc, retailer_code, item_id from"
					+ " PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc"
					+ " and a.retailer_code = "+Integer.parseInt(retailerNo)+" and DATE_FORMAT(b.AUDITTIME, '%Y/%m/%d')=DATE_FORMAT(now(),'%Y/%m/%d'));";
			
			
			/*String insertquery="insert into PRICE_DOT_COM_STAGING.OFFER_DELETE "
					+ "select MAPTABLE.COMMON_ID,MAPTABLE.UPC,MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID,now() "
					+ "from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN "
					+ "PRICE_DOT_COM_TARGET."+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE ="+retailerNo+"";
		*/	
			
			
			Statement stmt = conn.createStatement();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				esopp.partialDeleteCurl(rs.getString("UPC"), retailerName);
			}	
						
			stmt.execute(insertquery);
			
			logger.info("Deleted UPC list entered into PRICE_DOT_COM_STAGING.OFFER_DELETE table");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}	
	
	
	
	public void deletePartialRecord(Connection conn, String targetTableName,String retailerNo, String retailerName,String deleteQuery,String logpath){
		logger.addAppender(log.getLogProp(logpath));
		logger.info("Offer delete started for "+retailerName+ " retailer no "+retailerNo);
		try {
			//old select technique
		//	String query = "select MAPTABLE.UPC from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo);
			
			/*String query="select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC,"
					+ " MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE"
					+ " LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL"
					+ " and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo)+") as a "
					+ " where not exists  (Select common_id, upc, retailer_code, item_id from"
					+ " PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc and a.retailer_code = "+Integer.parseInt(retailerNo)+""
					+ " and DATE_FORMAT(b.AUDITTIME, '%Y/%m/%d')=DATE_FORMAT(now(), '%Y/%m/%d'))";*/
			
			String query = deleteQuery;

			/*String insertquery="insert into PRICE_DOT_COM_STAGING.OFFER_DELETE select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC,"
					+ " MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE"
					+ " LEFT OUTER JOIN "+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL"
					+ " and MAPTABLE.RETAILER_CODE = "+Integer.parseInt(retailerNo)+") as a where not exists  (Select common_id, upc, retailer_code, item_id from"
					+ " PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc"
					+ " and a.retailer_code = "+Integer.parseInt(retailerNo)+" and DATE_FORMAT(b.AUDITTIME, '%Y/%m/%d')=DATE_FORMAT(now(),'%Y/%m/%d'));";*/
			
			
			String insertquery ="insert into PRICE_DOT_COM_STAGING.OFFER_DELETE "+query;
			
			/*String insertquery="insert into PRICE_DOT_COM_STAGING.OFFER_DELETE "
					+ "select MAPTABLE.COMMON_ID,MAPTABLE.UPC,MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID,now() "
					+ "from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN "
					+ "PRICE_DOT_COM_TARGET."+targetTableName+" TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE ="+retailerNo+"";
		*/	
			
			
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				esopp.partialDeleteCurl(rs.getString("UPC"), retailerName);
			}	
			rs.close();
			stmt.close();
			
			Statement insertStmt = conn.createStatement();
			insertStmt.execute(insertquery);
			insertStmt.close();
			
			logger.info("Deleted UPC list entered into PRICE_DOT_COM_STAGING.OFFER_DELETE table");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.toString());
		}
		
	}
	
	
	/**
	 * @param conn
	 * @param affiliatorName
	 * @param stagingTableName
	 * @param targetTableName
	 * @param retailorNo
	 * @param insert
	 * @return
	 */
	public long targetTableCreation(Connection conn, String affiliatorName, String stagingTableName,
			String targetTableName, String retailorNo, boolean insert, String logfile) {
		long rowcnt = 0;
		if (affiliatorName.equalsIgnoreCase("linkshare")) {
			rowcnt = this.linkshareTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert, logfile);
		}else if(affiliatorName.equalsIgnoreCase("cj")){
			rowcnt = this.cjTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert, logfile);
		}else if(affiliatorName.equalsIgnoreCase("papperjam")){
			rowcnt = this.papperjamTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert, logfile);
		}else if(affiliatorName.equalsIgnoreCase("shareasale")){
			rowcnt = this.shareasaleTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert, logfile);
		}else if(affiliatorName.equalsIgnoreCase("impactradius")){
			rowcnt = this.impactrediusTargetTableCreation(conn, stagingTableName, targetTableName, retailorNo, insert, logfile);
		}
		return rowcnt;
	}

}
