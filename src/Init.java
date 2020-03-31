import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class Init {
    public static void main(final String[] args) {
        try {
            final String mongoDbUrl = "mongodb://127.0.0.1:27017";
            MongoClient mongoClient = MongoClients.create(mongoDbUrl);
            MongoDatabase database = mongoClient.getDatabase("test");
            CreateCollections coll = new CreateCollections(database);
            coll.createCollections();
            
            /*
             * @params (Object database_handle, Int - scale_factor) 
             */            
            InsertValues taskInsertValue = new InsertValues(database, 1);
            taskInsertValue.InsertValuesInCollection();
            
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