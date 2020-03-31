import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
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
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class Q1 {

	private MongoDatabase database;
	private static final Date baseDate = Date.valueOf("1998-12-01");
	private Instant startInstant;
	private Instant endInstant;

	public Q1(MongoDatabase db) {
		this.database = db;
	}

	public void executeQuery() {
		try {
//			Statement statement = connection.createStatement();
			Random random = new Random();
			int delta = random.nextInt(61) + 60;
			Date queryDate = new Date(baseDate.getTime() - delta * 86400000);
			this.startInstant = Instant.now();

			MongoCollection<Document> collection = database.getCollection("lineitem");

			Consumer<Document> printBlock = new Consumer<Document>() {
				@Override
				public void accept(final Document document) {
					System.out.println(document.toJson());
				}
			};
			
			MongoCursor<Document> myDoc = collection
					.find(lt("ship_date", queryDate))
					.projection(fields(include("return_flag", "line_status", "quantity", "extended_price", "discount"), excludeId()))
					.sort(Sorts.ascending("return_flag"))
					.sort(Sorts.ascending("line_status"))
					.forEach(printBlock);
			try {
				while (myDoc.hasNext()) {
					System.out.println(myDoc.next().toJson());
				}
			} finally {
				myDoc.close();
			}

//			statement.execute(
//					"select\n" + 
//					"l_returnflag,\n" + 
//					"l_linestatus,\n" + 
//					"sum(l_quantity) as sum_qty,\n" + 
//					"sum(l_extendedprice) as sum_base_price,\n" + 
//					"sum(l_extendedprice*(1-l_discount)) as sum_disc_price,\n" + 
//					"sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,\n" + 
//					"avg(l_quantity) as avg_qty,\n" + 
//					"avg(l_extendedprice) as avg_price,\n" + 
//					"avg(l_discount) as avg_disc,\n" + 
//					"count(*) as count_order\n" + 
//					"from\n" + 
//					"lineitem\n" + 
//					"where\n" + 
//					"l_shipdate <= date '"+queryDate+"'\n" + 
//					"group by\n" + 
//					"l_returnflag,\n" + 
//					"l_linestatus\n" + 
//					"order by\n" + 
//					"l_returnflag,\n" + 
//					"l_linestatus");

			this.endInstant = Instant.now();
			System.out.println("Execution time for Q1 is: " + Duration.between(startInstant, endInstant).toMillis()
					+ " miliseconds");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
