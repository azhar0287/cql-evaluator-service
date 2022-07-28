package org.opencds.cqf.cql.evaluator.cli.db;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.util.ThreadTaskCompleted;

import java.util.LinkedList;
import java.util.List;

import static com.mongodb.client.model.Projections.excludeId;

public class DbFunctions {
    public static Logger LOGGER  = LogManager.getLogger(DbFunctions.class);

    public List<Document> getConditionalData( String collectionName, int skip, int limit, DBConnection connection) {
        connection.collection = connection.database.getCollection(collectionName);
        FindIterable<Document> documents = connection.collection.find().skip(skip).limit(limit).batchSize(20000).projection(excludeId());
        MongoCursor<Document> cursor = documents.iterator();
        List<Document> list = new LinkedList<>();
        while(cursor.hasNext()) {
            list.add(cursor.next());
        }
        return list;
    }

    public List<Document> getSortedConditionalData( String collectionName, int skip, int limit, DBConnection connection) {
        String toBeSort="id";
        FindIterable<Document> documents = connection.database.getCollection(collectionName).find().skip(skip).limit(limit).batchSize(20000).projection(excludeId()).sort(new BasicDBObject(toBeSort,1));
        MongoCursor<Document> cursor = documents.iterator();
        List<Document> list = new LinkedList<>();
        while(cursor.hasNext()) {
            list.add(cursor.next());
        }
        return list;
    }

    public List<Document> getSinglePatient(String patientId, String collectionName, int skip, int limit, DBConnection connection) {
        connection.collection = connection.database.getCollection(collectionName);
        FindIterable<Document> documents = connection.collection.find(new Document("id", patientId)).projection(excludeId());
        MongoCursor<Document> cursor = documents.iterator();
        List<Document> list = new LinkedList<>();
        while(cursor.hasNext()) {
            list.add(cursor.next());
        }
        return list;
    }

    public List<Document> getRemainingData(String patientId, String collectionName, int skip, int limit, DBConnection connection) {
        connection.collection = connection.database.getCollection(collectionName);
        FindIterable<Document> documents = connection.collection.find().skip(skip).limit(limit).batchSize(20000).projection(excludeId());
        MongoCursor<Document> cursor = documents.iterator();
        List<Document> list = new LinkedList<>();
        while(cursor.hasNext()) {
            list.add(cursor.next());
        }
        return list;
    }

    public List<Document> getOidInfo(String code, String collectionName, DBConnection connection) {
        connection.collection = connection.database.getCollection(collectionName);
        FindIterable<Document> documents = connection.collection.find(new Document("values", code)).batchSize(10000);
        List<Document> list = new LinkedList<>();
        MongoCursor<Document> cursor = documents.iterator();
        while(cursor.hasNext()) {
            list.add(cursor.next());
        }
        return list;
    }

    public int getDataCount(String collectionName, DBConnection dbConnection) {
        dbConnection.collection = dbConnection.database.getCollection(collectionName);
        int count = Math.toIntExact(dbConnection.collection.count());
        return count;
    }

    public void insertProcessedDataInDb(String collectionName, List<Document> documents, DBConnection dbConnection) {
        dbConnection.collection = dbConnection.database.getCollection(collectionName);
        dbConnection.collection.insertMany(documents);
        LOGGER.info("Data batch has pushed: "+documents.size());
        documents.clear();
        documents = null;
    }

    public void insertFailedPatients(String collectionName, List<Document> documents, DBConnection dbConnection) {
        dbConnection.collection = dbConnection.database.getCollection(collectionName);
        if(documents.size()>0){
            dbConnection.collection.insertMany(documents);
            documents.clear();
            LOGGER.info("Data batch has pushed: "+documents.size());
        }
        LOGGER.info("No failed patients!! ");
    }


//    public void createIndexes(){
//        this.collection.createIndex(Indexes.ascending("id"));
//    }

    public boolean isAllTasksCompletedByThreads(List<ThreadTaskCompleted> isAllTasksCompleted){
        for(ThreadTaskCompleted isTaskCompleted : isAllTasksCompleted){
            if(isTaskCompleted.isTaskCompleted == false){
                return false;
            }
        }
        return true;
    }

}
