package org.opencds.cqf.cql.evaluator.cli.db;

import java.util.LinkedList;
import java.util.List;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Projections.excludeId;

public class DBConnection {

	private final MongoDatabase DB;
    private MongoCollection<org.bson.Document> collection ;

	public DBConnection() {
		MongoClient mongo = new MongoClient("10.20.30.212",27017);
		this.DB = mongo.getDatabase("ihm");
    }

	public List<Document> getConditionalData(String patientId, String collectionName, int skip, int limit) {
		this.collection = DB.getCollection(collectionName);
		//FindIterable<Document> documents = this.collection.find(new Document("id", patientId)).projection(excludeId());
		FindIterable<Document> documents = this.collection.find().skip(skip).limit(limit).projection(excludeId());

		MongoCursor<Document> cursor = documents.iterator();
		List<Document> list = new LinkedList<>();
		while(cursor.hasNext()) {
			list.add(cursor.next());
		}
		return list;
	}

	public long getDataCount(String collectionName) {
		this.collection = DB.getCollection(collectionName);
		long count = this.collection.count();
		return count;
	}

}
