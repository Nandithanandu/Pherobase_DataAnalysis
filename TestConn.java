import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.log4j.Logger;


public class TestConn {
	final static Logger log = Logger.getLogger(TestConn.class.getName());

	public boolean HiveCon(String ipAddress, String username,
			String password, String port) {
		Connection con =null;
		try{
			log.info("Entering into test connection method...");
			log.info("Trying to get connection from Hive...");
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			con = DriverManager.getConnection("jdbc:hive2://" + ipAddress + ":"
					+ port + "/default", username, password);
			
			log.info("HIVE IS successfully connected....");
			
			return true;	
			
		}catch(Exception ex){
			
			log.info("Exception while... "+ex.toString(), ex);
			
		}
		
		// TODO Auto-generated method stub
		return false;
	}

}
