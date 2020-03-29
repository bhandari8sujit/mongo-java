import com.mongodb.client.MongoDatabase;
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


public class CreateCollections {
	
	private MongoDatabase database;
	private MongoCollection<Document> customer;
	private MongoCollection<Document> lineItem;
	private MongoCollection<Document> nation;
	private MongoCollection<Document> orders;
	private MongoCollection<Document> part;
	private MongoCollection<Document> partSupplier;
	private MongoCollection<Document> region;
	private MongoCollection<Document> supplier;
	
	public CreateCollections(MongoDatabase db) {
		this.database = db;
	}	
	
	 String[] collectionNames = {
 			"part",
 			"supplier",
 			"partsupp",
 			"customer",
 			"orders",
 			"lineitem",
 			"nation",
 			"region",
 	};    

	public void createCollections() {
		 /*
	      * Delete all the collections, if they exist
	     */
	     
	     for (String name : database.listCollectionNames()) {
	     	if(name != null) {
	     		MongoCollection<Document> collection = database.getCollection(name);
	     		collection.drop();
	     		System.out.println("Collection " + name + " droppped!");
	     	}
	     }        
	     
	     /* 
	      * Get a handle to the collections / Create Collections
	      */       
	     
	     for(String name: collectionNames) {
	     	database.createCollection(name,
	     	          new CreateCollectionOptions().capped(false).autoIndex(true));
	     }
	     
	     customer = database.getCollection("customer");
	     lineItem = database.getCollection("lineitem");
	     nation = database.getCollection("nation");
	     orders = database.getCollection("orders");
	     part = database.getCollection("part");
	     partSupplier = database.getCollection("partsupp");
	     region = database.getCollection("region");
	     supplier = database.getCollection("supplier");		
	}
    
	
    
}
