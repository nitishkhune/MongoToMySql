package mongoToSql;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import com.mysql.jdbc.PreparedStatement;


public class DbTransfer {
	private static DB db;

	private static Connection conn;
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://130.65.133.181:3306/project2";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "password";

	private static DB connectToMongoDb() throws UnknownHostException {
		if (db == null) {
			MongoClient client = new MongoClient();
			db = client.getDB("logDb");
		}
		return db;
	}

	public static Connection connectToMySql() {
		 
		System.out.println("-------- MySQL JDBC Connection Testing ------------");
	 
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(URL,USERNAME,PASSWORD);
			if(conn!=null){
				System.out.println("Connected Succesfully");
			}

		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			
		}catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			
		}
		return conn;
	  }
	
	
	
	/*public static Connection connectToMySql() {
		if (conn == null) {
			try {
				Class.forName(DRIVER);
				conn = (Connection) DriverManager
						.getConnection(URL, USERNAME, PASSWORD);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return conn;
	}

*/	/*private static void archiveDataOfMongoDb() throws UnknownHostException {
		DBCollection tbl = connectToMongoDb().getCollection("projecttemp");
		Date today = new Date();
		String atblname = "archive"+today.getYear()+today.getMonth()+today.getDate();
		DBCollection atbl = connectToMongoDb().getCollection(atblname);
		DBCursor cur = tbl.find();
		while (cur.hasNext()) {
			atbl.insert(cur.next());
		}
		tbl.drop();
	}*/

	public static String getAggregateData() throws UnknownHostException {
		//DBCollection tbl = getConnection().getCollection("logs4");
		//tbl.rename("temp_logs4");
		DBCollection tbl = connectToMongoDb().getCollection("projecttemp");
		
		System.out.println("Inside MongoDb");
		/*String grp = "{$group:{_id:'$vmname',avgcpu:{$avg:'$cpu'},avgmemory:{$avg:'$memory'},avgdisk:{$avg:'$disk'},avgnetwork:{$avg:'$network'},avgsystem:{$avg:'$system'}}}";
		
		DBObject group = (DBObject) JSON.parse(grp);
		AggregationOutput output = tbl.aggregate(group);
		ArrayList<DBObject> list = (ArrayList<DBObject>) output.results();*/
		for (DBObject dbObject : tbl.find()) {
			//System.out.println(dbObject);
			
			System.out.println("TimeStamp:"+ dbObject.get("timestamp").toString());
			insertIntoMySql(dbObject);
		}
		
		return "";
	}

	public static void insertIntoMySql(DBObject obj) {
		try {
			
			PreparedStatement st = (PreparedStatement) connectToMySql().prepareStatement("insert into project2.vmLogStats(timestamp,vmname,cpu_usage,cpu_usageMHZ,total_write_latency,total_read_latency,disk_read,disk_write,disk_max_latency,disk_usage,memory_granted,memory_consumed,memory_active,vmmemctl,network_usage,network_received,network_transmitted,power,system_uptime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			System.out.println("**********************");
			System.out.println(obj.get("timestamp").toString());
			st.setString(1, obj.get("timestamp").toString());
			st.setString(2, obj.get("vmname").toString());
			st.setInt(3, Integer.parseInt(obj.get("cpu_usage").toString()));
			st.setInt(4, Integer.parseInt(obj.get("cpu_usagemhz").toString()));
			st.setInt(5, Integer.parseInt(obj.get("datastore_totalWriteLatency").toString()));
			st.setInt(6, Integer.parseInt(obj.get("datastore_totalReadLatency").toString()));
			st.setInt(7, Integer.parseInt(obj.get("disk_read").toString()));
			st.setInt(8, Integer.parseInt(obj.get("disk_usage").toString()));
			st.setInt(9, Integer.parseInt(obj.get("disk_write").toString()));
			st.setInt(10, Integer.parseInt(obj.get("disk_maxTotalLatency").toString()));
			st.setInt(11, Integer.parseInt(obj.get("mem_active").toString()));
			st.setInt(12, Integer.parseInt(obj.get("mem_granted").toString()));
			st.setInt(13, Integer.parseInt(obj.get("mem_vmmemctl").toString()));
			st.setInt(14, Integer.parseInt(obj.get("mem_consumed").toString()));
			st.setInt(15, Integer.parseInt(obj.get("net_usage").toString()));
			st.setInt(16, Integer.parseInt(obj.get("net_received").toString()));
			st.setInt(17, Integer.parseInt(obj.get("net_transmitted").toString()));
			st.setInt(18, Integer.parseInt(obj.get("power_power").toString()));
			st.setInt(19, Integer.parseInt(obj.get("sys_uptime").toString()));	
			st.executeUpdate();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	static Thread t1 = new Thread(){
		public void run(){
			while(true){
			try{
				connectToMySql();
			getAggregateData();
			System.out.println(" End one loop");
			Thread.sleep(300000);
			}catch(UnknownHostException e){
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}
	};

	public static void main(String[] args) throws UnknownHostException {
		t1.start();
	}
}
