import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class Init {
	
    public static void main(final String[] args) {     
    	try {
    		final String mongoDbUrl = "mongodb://127.0.0.1:27017";
        	MongoClient mongoClient = MongoClients.create(mongoDbUrl);
            MongoDatabase database = mongoClient.getDatabase("test");        
            CreateCollections coll = new CreateCollections(database);
            coll.createCollections();   
            InsertValues taskInsertValue = new InsertValues(database, 1);
    		taskInsertValue.InsertValues();
    		
            /* 
             * Close connection
             */
            mongoClient.close();
            
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    	
    } 
	

}