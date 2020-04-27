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

public class Q1 {

	private MongoDatabase database;
	private static final Date baseDate = Date.valueOf("1992-12-01");
	private Instant startInstant;
	private Instant endInstant;
	public static int rowCount = 0;

	public Q1(MongoDatabase db) {
		this.database = db;
	}

	public void executeQuery() {
		try {
			Random random = new Random();
			int delta = random.nextInt(61) + 60;
			Date queryDate = new Date(baseDate.getTime() - delta * 86400000);
			// ISODate("1992-01-05T03:52:19.542+0000")

			this.startInstant = Instant.now();
			MongoCollection<Document> collection = database.getCollection("lineitem");

			Consumer<Document> printBlock = new Consumer<Document>() {				
				@Override
				public void accept(final Document document) {
					Q1.rowCount++;
					System.out.println(document.toJson());					
				}
			};	

			List<? extends Bson> pipeline = Arrays.asList(
			new Document()
			      .append("$match", new Document()
			              .append("ship_date", new Document()
			                      .append("$lte", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").parse("1992-01-04 23:52:19.542-0400"))
			              )
			      ), 
			new Document()
			      .append("$group", new Document()
			              .append("_id", new Document()
			                      .append("line_status", "$line_status")
			                      .append("return_flag", "$return_flag")
			              )
			              .append("SUM(quantity)", new Document()
			                      .append("$sum", "$quantity")
			              )
			              .append("SUM(extended_price)", new Document()
			                      .append("$sum", "$extended_price")
			              )
			              .append("AVG(quantity)", new Document()
			                      .append("$avg", "$quantity")
			              )
			              .append("AVG(extended_price)", new Document()
			                      .append("$avg", "$extended_price")
			              )
			              .append("AVG(discount)", new Document()
			                      .append("$avg", "$discount")
			              )
			              .append("COUNT(*)", new Document()
			                      .append("$sum", 1)
			              )
			      ), 
			new Document()
			      .append("$project", new Document()
			              .append("return_flag", "$_id.return_flag")
			              .append("SUM(quantity)", "$SUM(quantity)")
			              .append("SUM(extended_price)", "$SUM(extended_price)")
			              .append("AVG(quantity)", "$AVG(quantity)")
			              .append("AVG(extended_price)", "$AVG(extended_price)")
			              .append("AVG(discount)", "$AVG(discount)")
			              .append("COUNT(*)", "$COUNT(*)")
			              .append("line_status", "$_id.line_status")
			              .append("_id", 0)
			      ), 
			new Document()
			      .append("$sort", new Document()
			              .append("return_flag", 1)
			              .append("line_status", 1)
			      ), 
			new Document()
			      .append("$project", new Document()
			              .append("_id", 0)
			              .append("return_flag", "$return_flag")
			              .append("SUM(quantity)", "$SUM(quantity)")
			              .append("SUM(extended_price)", "$SUM(extended_price)")
			              .append("AVG(quantity)", "$AVG(quantity)")
			              .append("AVG(extended_price)", "$AVG(extended_price)")
			              .append("AVG(discount)", "$AVG(discount)")
			              .append("COUNT(*)", "$COUNT(*)")
			      )
			);
			
			collection.aggregate(pipeline)
			.allowDiskUse(true)
			.forEach(printBlock);
			
//			collection.aggregate(
//				      Arrays.asList(
//				    	Aggregates.match(Filters.lte("ship_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").parse("1992-01-04 23:52:19.542-0400"))),
//				    	Aggregates.project(Document.parse("{sum_qty: {$sum: ['$quantity', 0]}}")),
//				    	Aggregates.project(Document.parse("{sum_base_price: {$sum: ['$extended_price', 0]}}"))				    	
//				      )				      
//				).forEach(printBlock); 
            
			
			/*
			 * Display all documents
			MongoCursor<Document> cursor = collection.find().projection(excludeId()).iterator();
			try {
				while (cursor.hasNext()) {
					rowCount++;					
					System.out.println(cursor.next().toJson());					
				}
			} finally {
				cursor.close();
			}
			
			*/
            
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q1 is: " + Duration.between(startInstant, endInstant).toMillis()
					+ " miliseconds");
			System.out.println("Number of documents: " + rowCount);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}



