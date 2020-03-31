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
            MongoDatabase database = mongoClient.getDatabase("test");
            CreateCollections coll = new CreateCollections(database);
                        
            startTable = Instant.now();            
            	coll.createCollections();            	
            endTable = Instant.now();
            System.out.println("Table Creation time is: "+ Duration.between(startTable, endTable).toMillis()+
					" miliseconds");            
            /*
             * @params (Object: database_handle, Int: scale_factor) 
             */            
            InsertValues taskInsertValue = new InsertValues(database, 1);
            
            startInsert = Instant.now();      
            taskInsertValue.InsertValuesInCollection();           
            endInsert = Instant.now();   
            System.out.println("Data Insertion time is: "+ Duration.between(startInsert, endInsert).toMillis()+
					" miliseconds");   
            /*
             * Close connection
             */
            mongoClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

//mongodb://127.0.0.1:27017/test?readPreference=primary&appname=MongoDB%20Compass&ssl=false
//Table Creation time is: 338 milliseconds
//Data Insertion time is: 1205269 milliseconds