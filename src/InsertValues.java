import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bson.Document;

public class InsertValues {

	private MongoDatabase database;
	private int sf;
	private Double[] retailPrices;

	private MongoCollection<Document> customer;
	private MongoCollection<Document> lineItem;
	private MongoCollection<Document> nation;
	private MongoCollection<Document> orders;
	private MongoCollection<Document> part;
	private MongoCollection<Document> partSupplier;
	private MongoCollection<Document> region;
	private MongoCollection<Document> supplier;

	List<Integer> region_key = new ArrayList<>();
	List<Integer> nation_key = new ArrayList<>();
	List<Integer> supplier_key = new ArrayList<>();
	List<Integer> part_key = new ArrayList<>();
	List<Integer> customer_key = new ArrayList<>();
	List<Integer> order_key = new ArrayList<>();

	public InsertValues(MongoDatabase db, int i) {
		this.database = db;
		this.sf = i;
		this.retailPrices = new Double[sf * 200000];
		this.customer = database.getCollection("customer");
		this.lineItem = database.getCollection("lineitem");
		this.nation = database.getCollection("nation");
		this.orders = database.getCollection("orders");
		this.part = database.getCollection("part");
		this.partSupplier = database.getCollection("partsupp");
		this.region = database.getCollection("region");
		this.supplier = database.getCollection("supplier");
	}

	private ArrayList<Integer> shuffleKey(Integer fixNum) {
		Integer size = new Integer(sf * fixNum);
		ArrayList<Integer> keys = new ArrayList<Integer>();
		for (int it = 0; it < size; it++) {
			keys.add(it);
		}
		Collections.shuffle(keys);
		return keys;
	}

	private Date addDays(Date date, int days) {
		return new Date(date.getTime() + days * 86400000);
	}

	public void InsertRegion() {
		String[] RegionNames = { "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST" };
		int itr = RegionNames.length;
		try {
			Random random = new Random();
			for (int i = 0; i < itr; i++) {
				String generatedString = random.ints(97, 122 + 1).limit(124)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
				this.region_key.add(i);
				this.region.insertOne(new Document("region_key", region_key.get(i))
						.append("name", i + "," + RegionNames[i]).append("comment", generatedString));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertNation() {
		String[] nationNamesRegionKeys = { "ALGERIA,0", "ARGENTINA,1", "BRAZIL,1", "CANADA,1", "EGYPT,4", "ETHIOPIA,0",
				"FRANCE,3", "GERMANY,3", "INDIA,2", "INDONESIA,2", "IRAN,4", "IRAQ,4", "JAPAN,2", "JORDAN,4", "KENYA,0",
				"MOROCCO,0", "MOZAMBIQUE,0", "PERU,1", "CHINA,2", "ROMANIA,3", "SAUDI ARABIA,4", "VIETNAM,2",
				"RUSSIA,3", "UNITED KINGDOM,3", "UNITED STATES,1" };
		int itr = nationNamesRegionKeys.length;
		try {
			Random random = new Random();
			for (int i = 0; i < itr; i++) {
				String generatedString = random.ints(97, 122 + 1).limit(124)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
				this.nation_key.add(i);
				this.nation.insertOne(new Document("nation_key", nation_key.get(i))
						.append("name", i + "," + nationNamesRegionKeys[i]).append("comment", generatedString));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertSupplier() {
		try {
			Integer size = sf * 10000;
			ArrayList<Integer> keys = shuffleKey(10000);
			Random random = new Random();
			ArrayList<Integer> CommentList = new ArrayList<Integer>();
			Integer commentedsize = 10 * sf;
			Integer index = 0;
			while (CommentList.size() < commentedsize) {
				index = random.nextInt(size);
				if (!CommentList.contains(index)) {
					CommentList.add(index);
				}
			}
			for (int i = 0; i < size; i++) {
				Integer key = keys.get(i);
				String formatted_key = String.format("%09d", key);
				int length = random.nextInt(30) + 10;
				String addressString = random.ints(97, 122 + 1).limit(length)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
				Integer nationInteger = random.nextInt(25);
				String phoneString = Integer.toString(nationInteger + 10) + "-"
						+ Integer.toString(random.nextInt(900) + 100) + "-"
						+ Integer.toString(random.nextInt(900) + 100) + "-"
						+ Integer.toString(random.nextInt(9000) + 1000);
				String acctbalString = String.format("%.2f", (random.nextInt(1099999) - 99999) / 100.0);
				String commentString = "No Comments";
				int commentIndex = CommentList.indexOf(key);
				String complaints = random.ints(97, 122 + 1).limit(length)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
				String recommends = random.ints(97, 122 + 1).limit(length)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
				if (commentIndex != -1) {
					if (commentIndex < 5 * sf) {
						commentString = "Customer" + complaints + "Complaints";
					} else {
						commentString = "Customer" + recommends + "Recommands";
					}
				}

				this.supplier.insertOne(new Document("supplier_key", key).append("name", "SUpplier#r" + formatted_key)
						.append("address", addressString).append("nation_key", nationInteger)
						.append("phone", phoneString).append("acct_bal", acctbalString)
						.append("comment", commentString));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertPart() {

		String[] pNames = { "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
				"blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower",
				"cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest",
				"frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory",
				"khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium",
				"metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid",
				"pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy",
				"royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring",
				"steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow" };
		String[] typeSyllables1 = { "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO" };
		String[] typeSyllables2 = { "ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED" };
		String[] typeSyllables3 = { "TIN", "NICKEL", "BRASS", "STEEL", "COPPER" };
		String[] containerSyllables1 = { "SM", "LG", "MED", "JUMBO", "WRAP" };
		String[] containerSyllables2 = { "CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM" };

		try {			
			Integer size = sf * 200000;
			Random random = new Random();
			ArrayList<Integer> keys = shuffleKey(200000);

			for (int i = 0; i < size; i++) {
				Integer key = keys.get(i);

				ArrayList<String> selectedNames = new ArrayList<String>();
				int length = pNames.length;
				while (selectedNames.size() < 5) {
					int index = random.nextInt(length);
					if (!selectedNames.contains(pNames[index])) {
						selectedNames.add(pNames[index]);
					}
				}
				String nameString = String.join(" ", selectedNames);

				int M = random.nextInt(5) + 1;

				String mfgrString = "Manufacturer#" + M;

				String brandString = "Brand#" + String.valueOf(M) + String.valueOf((random.nextInt(5) + 1));

				String typeString = typeSyllables1[random.nextInt(6)] + " " + typeSyllables2[random.nextInt(5)] + " "
						+ typeSyllables3[random.nextInt(5)];

				String sizeString = String.valueOf(random.nextInt(50) + 1);

				String containerString = containerSyllables1[random.nextInt(5)] + " "
						+ containerSyllables2[random.nextInt(8)];

				Double price = (90000 + (key / 10) % 20001 + 100 * key % 1000) / 100.0;
				retailPrices[key] = price;
				String retailPriceString = String.format("%.2f", price);

				String pCommentString = random.ints(97, 122 + 1).limit(length)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

				this.part.insertOne(new Document("part_key", key)
						.append("name", nameString)
						.append("mfgr", mfgrString)
						.append(key, value)
						);
				
//				statement.executeUpdate("UPSERT INTO PART VALUES(" + key + ",'" + nameString + "','" + mfgrString
//						+ "','" + brandString + "','" + typeString + "'," + sizeString + ",'" + containerString + "',"
//						+ retailPriceString + ",'" + pCommentString + "')");
				
//				if (i%9000==0) {
//					connection.commit();
//				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertValuesInCollection() {
		try {

			System.out.println("Inserting values into region collection");
			InsertRegion();
			System.out.println("Values Added to region collection");

			System.out.println("Inserting values into nation collection");
			InsertNation();
			System.out.println("Values Added to nation collection");

			System.out.println("Inserting values into supplier collection");
			InsertSupplier();
			System.out.println("Values Added to supplier collection");
			
			System.out.println("Inserting values into part collection");
			InsertPart();
			System.out.println("Values Added to part collection");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
