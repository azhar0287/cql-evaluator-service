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
	private final MongoDatabase DB;
    private MongoCollection<org.bson.Document> collection ;

	public DBConnection() {
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
		//build the connection options
		builder.maxConnectionIdleTime(86400000);//set the max wait time in (ms)

		MongoClientOptions opts = builder.build();
		MongoClient mongo = new MongoClient(new ServerAddress("10.20.30.212",27017), opts);
		this.DB = mongo.getDatabase("ihm");
    }

	public List<Document> getConditionalData(String patientId, String collectionName, int skip, int limit) {
		this.collection = DB.getCollection(collectionName);
		//FindIterable<Document> documents = this.collection.find(new Document("id", patientId)).projection(excludeId());
		FindIterable<Document> documents = this.collection.find().skip(skip).limit(limit).batchSize(20000).projection(excludeId());

		MongoCursor<Document> cursor = documents.iterator();

		List<Document> list = new LinkedList<>();
		while(cursor.hasNext()) {

			list.add(cursor.next());
		}
		return list;
	}

	public List<Document> getOidInfo(String code, String collectionName) {
		this.collection = DB.getCollection(collectionName);
		FindIterable<Document> documents = this.collection.find(new Document("values", code)).batchSize(10000);
		List<Document> list = new LinkedList<>();
		MongoCursor<Document> cursor = documents.iterator();
		while(cursor.hasNext()) {

			list.add(cursor.next());
		}
		return list;
	}

	public int getDataCount(String collectionName) {
		this.collection = DB.getCollection(collectionName);
		int count = Math.toIntExact(this.collection.count());
		return count;
	}

	public void insertProcessedDataInDb(String collectionName, List<Document> documents) {
		this.collection = DB.getCollection(collectionName);
		//this.collection.createIndex(Indexes.ascending("id"));
		this.collection.insertMany(documents);
//		this.collection.bulkWrite(documents);
		LOGGER.info("Data batch has pushed: "+documents.size());
		documents.clear();
		documents = null;
		documents = new ArrayList<>();
	}

	public void createIndexes(){
		this.collection.createIndex(Indexes.ascending("id"));
	}

}
