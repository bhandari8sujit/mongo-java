import com.mongodb.client.MongoDatabase;

public class InsertValues {

	private MongoDatabase database;
	private int sf;
	private Double[] retailPrices;
	
	public InsertValues(MongoDatabase db, int i) {
		this.database = db;
		this.sf = i;
		this.retailPrices = new Double[sf*200000];
	}

	public void InsertValues() {
		
	}

}
