import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class Query {
	public static void main(String[] args) {
		try {
			final String mongoDbUrl = "mongodb://127.0.0.1:27017";
            MongoClient mongoClient = MongoClients.create(mongoDbUrl);
            MongoDatabase database = mongoClient.getDatabase("test");            
            
            Q1 q1Task = new Q1(database);            
            q1Task.executeQuery();
            
            Q2 q2Task = new Q2(database);            
            q2Task.executeQuery();
            
            mongoClient.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
