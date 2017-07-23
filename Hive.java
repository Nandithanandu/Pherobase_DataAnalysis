import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;


public class Hive {

	static final Logger log = Logger.getLogger(Hive.class.getName()); 
public static void main(String args[]){
	InputStream input = null; 
		
		try{
			
			log.info("Entering into execution method...");
			Properties prop = new Properties();
			input= new FileInputStream("/home/cloudera/workspace/Hadoop/input/inputs.properties");
			prop.load(input);
			
			String Ipaddress = prop.getProperty("Ipaddress");
			String port = prop.getProperty("port");
			String uname = prop.getProperty("username");
			String pwd = prop.getProperty("password");
			String Schema = prop.getProperty("schema");
			String nschema = prop.getProperty("newschema");
			String cnschema = prop.getProperty("createnewschema");
			String delimiter = prop.getProperty("Delimiter");
			String tablename = prop.getProperty("tablename");
			String sparkEnable = prop.getProperty("SPARK_ENABLE");
			
			
			
			String namenodeip = prop.getProperty("namenodeIp");
			String source = prop.getProperty("sourcefile");
			String sourcedel = prop.getProperty("sourceDel");
			
			String tableQuery = "create table "+tablename+"(compoundsortedbychainlength string,MW string,chainlength string,functionalgroup string,order string,family string,subfamily string,species string,categoryofchemicals string,generalnameofinsects string) row format delimited "
					+ "fields terminated by '"+delimiter+"'";
	     
			HiveDat data = new HiveDat();
			data.setCreateNewSchema(Schema);
			data.setSourceDel(sourcedel);
			data.setTableName(tablename);
			data.setTableQuery(tableQuery);
			data.setNewSchemaName(nschema);
			data.setCreateNewSchema(cnschema);
			
			
			boolean flag=data.hiveDat(Ipaddress, port, uname, pwd, namenodeip, source,sparkEnable);
			if(flag){
				System.out.println("Data transfer completed");
			}else{
			System.out.println("Data transfer is not completed");
			}
			log.info("Completed into execution method...");
			}
		catch(Exception ex){
			log.info("Exception while "+ex.toString(), ex);	
		}
	
	}
}
