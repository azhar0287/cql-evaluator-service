package org.opencds.cqf.cql.evaluator.cli.db;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import com.mongodb.client.model.Indexes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.opencds.cqf.cql.evaluator.cli.command.CqlCommand;
import org.opencds.cqf.cql.evaluator.cli.mappers.SheetInputMapper;

import static com.mongodb.client.model.Projections.excludeId;

public class DBConnection {
	public static Logger LOGGER  = LogManager.getLogger(DBConnection.class);
	public MongoDatabase database;
    public MongoCollection<org.bson.Document> collection ;
	MongoClient mongoClient;

	public DBConnection() {
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
		//build the connection options
		builder.maxConnectionIdleTime(86400000);//set the max wait time in (ms)

		MongoClientOptions opts = builder.build();
		mongoClient = new MongoClient(new ServerAddress("10.20.30.212",27017), opts);
		this.database = mongoClient.getDatabase("ihm");
    }

}
