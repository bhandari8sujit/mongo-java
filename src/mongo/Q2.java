package mongo;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
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
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class Q2 {

	private MongoDatabase database;
	private static final Date baseDate = Date.valueOf("1992-12-01");
	private Instant startInstant;
	private Instant endInstant;
	public static int rowCount = 0;
	
private static final String[] typeSyllables3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
	
	private static final String[] RegionNames = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};

	public Q2(MongoDatabase db) {
		this.database = db;
	}

	public void executeQuery() {
		try {
			
			Random random = new Random();
			String typeString = typeSyllables3[random.nextInt(5)];
			String regionString = RegionNames[random.nextInt(5)];
			Integer sizeInteger = random.nextInt(50)+1;
			
			Consumer<Document> printBlock = new Consumer<Document>() {				
				@Override
				public void accept(final Document document) {
					Q2.rowCount++;
					System.out.println(document.toJson());					
				}
			};
			
		    MongoCollection<Document> collection = database.getCollection("lineitem");
			
			this.startInstant = Instant.now();
					
//			 List<? extends Bson> pipeline = Arrays.asList(
//	                    new Document()
//	                            .append("$match", new Document()
//	                                    .append("$and", Arrays.asList(
//	                                            new Document()
//	                                                    .append("l_shipdate", new Document()
//	                                                            .append("$gte", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").parse("1997-07-01 00:52:19.542-0300"))
//	                                                    ),
//	                                            new Document()
//	                                                    .append("l_ship_date", new Document()
//	                                                            .append("$lt", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").parse("1998-12-31 23:52:19.542-0400"))
//	                                                    ),
//	                                            new Document()
//	                                                    .append("discount", new Document()
//	                                                            .append("$gte", 0.04)
//	                                                    ),
//	                                            new Document()
//	                                                    .append("discount", new Document()
//	                                                            .append("$lte", 0.06)
//	                                                    ),
//	                                            new Document()
//	                                                    .append("quantity", new Document()
//	                                                            .append("$lt", 24L)
//	                                                    )
//	                                        )
//	                                    )
//	                            ), 
//	                    new Document()
//	                            .append("$group", new Document()
//	                                    .append("_id", new Document())
//	                                    .append("SUM(extended_price)", new Document()
//	                                            .append("$sum", "$extended_price")
//	                                    )
//	                            ), 
//	                    new Document()
//	                            .append("$project", new Document()
//	                                    .append("SUM(extended_price)", "$SUM(extended_price)")
//	                                    .append("_id", 0)
//	                            )
//	            );
//	            
//	            collection.aggregate(pipeline)
//	                    .allowDiskUse(true)
//	                    .forEach(printBlock);
			
			 collection.aggregate(
		                Arrays.asList(
		                        Aggregates.match(Filters.gte("ship_date", 27)),
		                        Aggregates.match(Filters.lt("ship_date", 11)),
		                        Aggregates.match(Filters.gte("discount", 0.04)),
		                        Aggregates.match(Filters.lte("discount", 0.06)),
		                        Aggregates.match(Filters.lt("quantity", 24))
		                        
		                		)
		     ).forEach(printBlock);
			            
			this.endInstant = Instant.now();
			
			System.out.println("Execution time for Q2 is: " + Duration.between(startInstant, endInstant).toMillis()
					+ " miliseconds");
			System.out.println("Number of documents: " + rowCount);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
