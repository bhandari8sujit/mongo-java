package mongo;

import java.time.Duration;
import java.time.Instant;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class Init {
    public static void main(final String[] args) {    	
        try {
        	Instant startTable;
        	Instant endTable;
        	Instant startInsert;
        	Instant endInsert;
            final String mongoDbUrl = "mongodb://127.0.0.1:27017";
            MongoClient mongoClient = MongoClients.create(mongoDbUrl);
            
            /*Create a db called test, if it doesn't exist already*/
            
            MongoDatabase database = mongoClient.getDatabase("test");
            
            
            CreateCollections coll = new CreateCollections(database);
                        
            startTable = Instant.now();            
            	coll.createCollections();            	
            endTable = Instant.now();
            System.out.println("Collection Creation time is: "+ Duration.between(startTable, endTable).toMillis()+
					" miliseconds");            
            
            /*
             * @params (Object: database_handle, Int: scale_factor)             * 
             * We're using 3 scale factors for comparisons 1, 0.05, 0.01
             */
            
//             InsertValues taskInsertValue = new InsertValues(database, 1);
             InsertValues taskInsertValue = new InsertValues(database, 0.05);
//             InsertValues taskInsertValue = new InsertValues(database, 0.01);

            startInsert = Instant.now();      
            taskInsertValue.InsertValuesInCollection();           
            endInsert = Instant.now();   
            System.out.println("Data Insertion time is: "+ Duration.between(startInsert, endInsert).toMillis()+	" miliseconds");
            
            Q1 q1Task = new Q1(database);            
            q1Task.executeQuery();
        
            mongoClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
