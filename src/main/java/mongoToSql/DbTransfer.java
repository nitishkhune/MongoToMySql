package mongoToSql;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
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
	private static Date startTime = null;
	private static Connection conn;
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://localhost:3306/project2";
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
		
		String grp = "{$group:{_id:'$vmname',avgcpu:{$avg:'$cpu_usage'},avgcpumhz:{$avg:'$cpu_usagemhz'},"
				+ "avgWriteLatency:{$avg:'$datastore_totalWriteLatency'},"
				+ "avgReadLatency:{$avg:'$datastore_totalReadLatency'},"
				+ "avgDiskWrite:{$avg:'$disk_write'},"
				+ "avgDiskRead:{$avg:'$disk_read'},"
				+ "avgDiskMaxTotalLatency:{$avg:'$disk_maxTotalLatency'},"
				+ "avgDiskUsage:{$avg:'$disk_usage'},"
				+ "avgMemGranted:{$avg:'$mem_granted'},"
				+ "avgMemConsumed:{$avg:'$mem_consumed'},"
				+ "avgMemActive:{$avg:'$mem_active'},"
				+ "avgMemVMMemCtl:{$avg:'$mem_vmmemctl'},"
				+ "avgNetworkUsage:{$avg:'$net_usage'},"
				+ "avgNetworkReceived:{$avg:'$net_received'},"
				+ "avgNetworkTransmitted:{$avg:'$net_transmitted'},"
				+ "avgPower:{$avg:'$power_power'},"
				+ "avgSysUptime:{$avg:'$sys_uptime'}}}";
		
		DBObject group = (DBObject) JSON.parse(grp);
		AggregationOutput output = tbl.aggregate(group);
		ArrayList<DBObject> list = (ArrayList<DBObject>) output.results();
		//DBObject tempDbObject = null;
		for (DBObject dbObject : list) {
			System.out.println("-->"+dbObject);
			//System.out.println("TimeStamp:"+ dbObject.get("timestamp").toString());
			insertIntoMySql(dbObject);
		}
		
		return "";
	}

	public static void insertIntoMySql(DBObject obj) {
		try {
			
			PreparedStatement st = (PreparedStatement) connectToMySql().prepareStatement("insert into project2.vmLogStats(timestamp,vmname,cpu_usage,cpu_usageMHZ,total_write_latency,total_read_latency,disk_write,disk_read,disk_max_latency,disk_usage,memory_granted,memory_consumed,memory_active,vmmemctl,network_usage,network_received,network_transmitted,power,system_uptime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			//System.out.println("**********************");
			//System.out.println(obj.get("timestamp").toString());
			st.setString(1, obj.get("timestamp").toString());
			st.setString(2, obj.get("vmname").toString());
			st.setInt(3, Integer.parseInt(obj.get("avgcpu").toString()));
			st.setInt(4, Integer.parseInt(obj.get("avgcpumhz").toString()));
			st.setInt(5, Integer.parseInt(obj.get("avgWriteLatency").toString()));
			st.setInt(6, Integer.parseInt(obj.get("avgReadLatency").toString()));
			st.setInt(7, Integer.parseInt(obj.get("avgDiskWrite").toString()));
			st.setInt(8, Integer.parseInt(obj.get("avgDiskRead").toString()));
			st.setInt(9, Integer.parseInt(obj.get("avgDiskMaxTotalLatency").toString()));
			st.setInt(10, Integer.parseInt(obj.get("avgDiskUsage").toString()));
			st.setInt(11, Integer.parseInt(obj.get("avgMemGranted").toString()));
			st.setInt(12, Integer.parseInt(obj.get("avgMemConsumed").toString()));
			st.setInt(13, Integer.parseInt(obj.get("avgMemActive").toString()));
			st.setInt(14, Integer.parseInt(obj.get("avgMemVMMemCtl").toString()));
			st.setInt(15, Integer.parseInt(obj.get("avgNetworkUsage").toString()));
			st.setInt(16, Integer.parseInt(obj.get("avgNetworkReceived").toString()));
			st.setInt(17, Integer.parseInt(obj.get("avgNetworkTransmitted").toString()));
			st.setInt(18, Integer.parseInt(obj.get("avgPower").toString()));
			st.setInt(19, Integer.parseInt(obj.get("avgSysUptime").toString()));	
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
