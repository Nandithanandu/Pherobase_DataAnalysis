import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import utils.Helper;
import utils.sparkTest;




public class HiveDat {
	final static Logger log = Logger.getLogger(HiveData.class.getName());
	private static String schemaName;
	private static String tableQuery;
	private static String tableName;
	private static String sourceDel;
	private static String newSchema;
	private static String newSchemaName;
	private static String createNewSchema;
	public Helper helper;
	
	public static String getCreateNewSchema() {
		return createNewSchema;
	}

	public static void setCreateNewSchema(String createNewSchema) {
		HiveDat.createNewSchema = createNewSchema;
	}

	public static String getSchemaName() {
		return schemaName;
	}

	public static void setSchemaName(String schemaName) {
		HiveDat.schemaName = schemaName;
	}

	public static String getTableQuery() {
		return tableQuery;
	}

	public static void setTableQuery(String tableQuery) {
		HiveDat.tableQuery = tableQuery;
	}

	public static String getTableName() {
		return tableName;
	}

	public static void setTableName(String tableName) {
		HiveDat.tableName = tableName;
	}

	public static String getSourceDel() {
		return sourceDel;
	}

	public static void setSourceDel(String sourceDel) {
		HiveDat.sourceDel = sourceDel;
	}

	public static String getNewSchema() {
		return newSchema;
	}

	public static void setNewSchema(String newSchema) {
		HiveDat.newSchema = newSchema;
	}

	public static String getNewSchemaName() {
		return newSchemaName;
	}

	public static void setNewSchemaName(String newSchemaName) {
		HiveDat.newSchemaName = newSchemaName;
	}

	public static Logger getLog() {
		return log;
	}

	
	public boolean hiveDat(String IpAddress, String port, String username,
			String password, String namenodeIP, String sourcefile,
			String sparkEnable) throws SQLException {
		
		Connection con = null;
		PreparedStatement stmt1 = null;
		PreparedStatement stmt2 = null;
		ResultSet rset = null;
		try {
			
			log.info("Entering into hiveData Connection...");
			helper = new Helper();
			log.info("Trying to get Connection from Hive");
			TestConn test = new TestConn();
			boolean conn = test.HiveCon(IpAddress, username,
					password, port);
			if (conn) {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				con = DriverManager.getConnection("jdbc:hive2://" + IpAddress
						+ ":" + port + "/" + schemaName, username, password);

				if (createNewSchema != null && createNewSchema.equals("Y")) {

					log.info("Trying to create table with new schema.. ");
					stmt1 = con.prepareStatement("drop database if exists "
							+ newSchemaName);
					stmt1.execute();

					helper.releaseResource(null, stmt1, null);
					stmt1 = con.prepareStatement("create database "
							+ newSchemaName);
					stmt1.execute();

					helper.releaseResource(null, stmt1, null);
					
					stmt1 = con.prepareStatement("use " + newSchemaName);
					stmt1.execute();

					helper.releaseResource(null, stmt1, null);

					stmt1 = con.prepareStatement("drop table if exists "
							+ tableName);
					stmt1.execute();

					helper.releaseResource(null, stmt1, null);
					stmt1 = con.prepareStatement(tableQuery);
					stmt1.execute();
					log.info("Table created with table name " + tableName
							+ " in Hive");

					helper.releaseResource(null, stmt1, null);
					
	}
				 else {

						stmt1 = con.prepareStatement("drop table if exists "
								+ tableName);
						stmt1.execute();

						helper.releaseResource(null, stmt1, null);

						log.info("Trying to create table ");
						stmt1 = con.prepareStatement(tableQuery);
						stmt1.execute();
						log.info("Table created with table name " + tableName
								+ " in Hive");

						helper.releaseResource(null, stmt1, null);
					}
				log.info("Trying to fetch hdfs details.");
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", "hdfs://" + namenodeIP + ":8020");

				FileSystem fs = FileSystem.newInstance(new URI("hdfs://"
						+ namenodeIP + ":8020"), conf);
				File file = new File(sourcefile);
				String fileName = file.getName();

				Path sourcePath = new Path(sourcefile);
				Path targetPath = null;
				if (createNewSchema != null && createNewSchema.equals("Y")) {
					targetPath = new Path("/user/hive/warehouse/"
							+ newSchemaName.toLowerCase() + ".db/"
							+ tableName.toLowerCase() + "/" + fileName);
				} else {

					targetPath = new Path("/user/hive/warehouse/"
							+ tableName.toLowerCase() + "/" + fileName);
				}
				if (fs.exists(targetPath)) {
					log.info("File already exists....  Please try to give with another target file name");
					fs.copyFromLocalFile(false, false, sourcePath, targetPath);
				} else if (sourceDel != null && sourceDel.equals("Y")) {
					fs.copyFromLocalFile(true, false, sourcePath, targetPath);
				} else {
					fs.copyFromLocalFile(false, false, sourcePath, targetPath);
				}
				log.info("Files are copied into desired path");

				sparkTest spark = new sparkTest();

				if (createNewSchema != null && createNewSchema.equals("Y")) {
					helper.releaseResource(null, stmt1, null);
					if (sparkEnable != null && sparkEnable.equals("Y")) {

						boolean Flag = spark.sparkPerf(IpAddress, port,
								username, password, newSchemaName, tableName,
								schemaName, createNewSchema);
						log.info("Flag value for spark count is :: "+Flag);
					} else {

						helper.releaseResource(null, stmt1, null);

						long lStartTime = System.nanoTime();
						System.out.println(lStartTime);

						stmt1 = con.prepareStatement("select count(*) from "
								+ newSchemaName.toLowerCase() + "."
								+ tableName.toLowerCase());
						rset = stmt1.executeQuery();

						while (rset != null && rset.next()) {
							log.info("row count for table is.... "
									+ rset.getString(1));
						}

						long lEndTime = System.nanoTime();
						System.out.println(lEndTime);

						// time elapsed
						long output = (lEndTime - lStartTime);

						System.out.println("Elapsed time in seconds: "
								+ (output / 1000000) * (0.001));

					}
					log.info("Table created with table name " + tableName
							+ " in Hive");

					helper.releaseResource(null, stmt1, rset);

				} else {
					helper.releaseResource(null, stmt1, rset);

					if (sparkEnable != null && sparkEnable.equals("Y")) {

						boolean Flag = spark.sparkPerf(IpAddress, port,
								username, password, newSchemaName, tableName,
								schemaName, createNewSchema); 
						log.info("Flag value for spark count is :: "+Flag);

					} else {
						helper.releaseResource(null, stmt1, null);

						long lStartTime = System.nanoTime();
						System.out.println(lStartTime);

						stmt1 = con.prepareStatement("select count(*) from "
								+ tableName);
						rset = stmt1.executeQuery();

						while (rset != null && rset.next()) {
							log.info("row count for table is.... "
									+ rset.getString(1));
						}

						long lEndTime = System.nanoTime();
						System.out.println(lEndTime);

						// time elapsed
						long output = (lEndTime - lStartTime);

						System.out.println("Elapsed time in seconds: "
								+ (output / 1000000) * (0.001));

						log.info("Table created with table name " + tableName
								+ " in Hive");

						helper.releaseResource(null, stmt1, rset);
					}
				}
			} else {
				log.info("Not able to get the connection... Please check log for further Details....");
			}
			return true;
				
		}catch (Exception ex) {
		log.info("Exception while " + ex.toString(), ex);
		return false;
		}
	
		finally {
			
			try {
				log.info("Trying to close connection");

				if (con != null) {
					con.close();
					log.info("Connection closed...");
				}
			} catch (Exception ex) {
				log.info("Exception while doing... " + ex.toString(), ex);
			}
		}

	}
	
}

	
	

	