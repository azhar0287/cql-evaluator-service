package org.opencds.cqf.cql.evaluator.cli.db;



import com.mongodb.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class DBConnection {
	public static Logger LOGGER  = LogManager.getLogger(DBConnection.class);
	public MongoDatabase database;
    public MongoCollection<org.bson.Document> collection ;
	MongoClient mongoClient;

	public DBConnection() {
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
		//build the connection options
		builder.maxConnectionIdleTime(96400000);//set the max wait time in (ms)
		builder.retryWrites(true);
		builder.readPreference(ReadPreference.primaryPreferred());
//		builder.socketKeepAlive(true).connectTimeout(96400000);
		builder.socketTimeout(96400000);
		//builder.threadsAllowedToBlockForConnectionMultiplier(10);
		MongoClientOptions opts = builder.build();

		if(mongoClient == null) {
			mongoClient = new MongoClient(new ServerAddress("10.20.30.212",27017), opts);
			this.database = mongoClient.getDatabase("ihm");
		}
		else {
			System.out.println("Connection already created: ");
		}
    }

	public void closeConnection() {
		this.mongoClient.close();
	}
	
}
