package mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

public class CreateCollections {

	private MongoDatabase database;

	public CreateCollections(MongoDatabase db) {
		this.database = db;
	}

	String[] collectionNames = { "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation",
			"region", };

	public void createCollections() {
		/*
		 * Delete all the collections, if they exist
		 */
		for (String name : database.listCollectionNames()) {
			if (name != null) {
				MongoCollection<Document> collection = database.getCollection(name);
				collection.drop();
				System.out.println("Collection " + name + " droppped!");
			}
		}
		/*
		 * Create Collections
		 */
		for (String name : collectionNames) {
			database.createCollection(name, new CreateCollectionOptions().capped(false).autoIndex(true));
		}
	}

}
