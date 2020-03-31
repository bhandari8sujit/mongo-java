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
		this.retailPrices = new Double[this.sf * 200000];
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
				this.region.insertOne(new Document("region_key", i)
						.append("name", i + "," + RegionNames[i])
						.append("comment", generatedString));
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
				this.nation.insertOne(new Document("nation_key", i)
						.append("name", i + "," + nationNamesRegionKeys[i])
						.append("comment", generatedString));
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

				this.supplier.insertOne(new Document("supplier_key", key)
						.append("name", "SUpplier#r" + formatted_key)
						.append("address", addressString)
						.append("nation_key", nationInteger)
						.append("phone", phoneString)
						.append("acct_bal", acctbalString)
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
						.append("brand", brandString)
						.append("type", typeString)
						.append("size", sizeString)
						.append("container", containerString)
						.append("retail_price", retailPriceString)
						.append("comment", pCommentString));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertPartSupp() {
		try {
			Integer S = sf * 10000;
			Integer size = sf * 200000;
			Random random = new Random();
			ArrayList<Integer> psKeys = shuffleKey(200000);
			for (int idx = 0; idx < size; idx++) {
				Integer psKey = psKeys.get(idx);
				for (int i = 0; i < 4; i++) {
					Integer psSuppKey = (psKey + (i * ((S / 4) + (psKey - 1) / S))) % S;
					Integer psAvailQTY = random.nextInt(9999) + 1;
					String psSupplyCost = String.format("%.2f", (random.nextInt(99901) + 100) / 100.0);
					int length = random.nextInt(150) + 49;
					String psComment = random.ints(97, 122 + 1).limit(length)
							.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
							.toString();

					this.partSupplier.insertOne(new Document("part_key", psKey)
							.append("supp_key", psSuppKey)
							.append("avail_qty", psAvailQTY)
							.append("supply_cost", psSupplyCost)
							.append("comment", psComment)
							);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertCustomer() {

		String[] Segments = { "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD" };

		try {
			Integer size = sf * 150000;
			Random random = new Random();
			ArrayList<Integer> keys = shuffleKey(150000);
			for (int i = 0; i < size; i++) {
				Integer key = keys.get(i);
				String cNameString = "Customer#" + String.format("%09d", key);
				int length = random.nextInt(30) + 10;
				String addressString = random.ints(97, 122 + 1).limit(length)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
				Integer nationInteger = random.nextInt(25);
				String phoneString = Integer.toString(nationInteger + 10) + "-"
						+ Integer.toString(random.nextInt(900) + 100) + "-"
						+ Integer.toString(random.nextInt(900) + 100) + "-"
						+ Integer.toString(random.nextInt(9000) + 1000);
				String acctbalString = String.format("%.2f", (random.nextInt(1099999) - 99999) / 100.0);
				String mktSegmentString = Segments[random.nextInt(5)];
				Integer count = random.nextInt(88) + 29;
				String commentString = random.ints(97, 122 + 1).limit(count)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

				this.customer.insertOne(new Document("cust_key", key)
						.append("name", cNameString)
						.append("address", addressString)
						.append("nation_key", nationInteger)
						.append("phone", phoneString)
						.append("acct_bal", acctbalString)
						.append("mkt_segment", mktSegmentString)
						.append("comment", commentString)
						);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void InsertOrderLineItem() {

		String[] priorities = { "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW" };
		String[] instructs = { "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN" };
		String[] modes = { "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB" };
		Date startDate = Date.valueOf("1992-01-01");
		Date currentDate = Date.valueOf("1995-06-17");
		Date endDate = Date.valueOf("1998-12-31");
		try {
			Integer orderSize = sf * 1500000 * 4;
			ArrayList<Integer> orderKeys = shuffleKey(1500000 * 4);
			Random random = new Random();
			for (int idx = 0; idx < orderSize; idx++) {
				Integer orderKey = orderKeys.get(idx);
				if (orderKey % 4 != 0) {
					continue;
				}
				Integer custKey = 0;
				do {
					custKey = random.nextInt(sf * 150000);
				} while (custKey % 3 == 0);
				// set to P as default, will change according to lineItems
				String orderStatusString = "P";
				// set to 0.0 as place holder, will change according to lineItems
				Double totalPrice = 0.0;
				// endDate - 151 days
				Date date = addDays(endDate, -151);
				long raw = date.getTime();
				long startRaw = startDate.getTime();
				int difference = (int) (raw - startRaw);
				Date orderDate = new Date(startRaw + random.nextInt(difference));
				String priorityString = priorities[random.nextInt(5)];
				String clerkString = "Clerk#" + String.format("%09d", random.nextInt(sf * 1000) + 1);
				String shipPriorityString = "0";
				String oCommentString = random.ints(97, 122 + 1).limit(124)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

				int lineItemRows = random.nextInt(7) + 1;
				int Fnumber = 0;
				int Onumber = 0;
				for (int j = 0; j < lineItemRows; j++) {
					Integer L_OrderKey = orderKey;
					Integer partKey = random.nextInt(sf * 200000);
					int S = sf * 10000;
					int i = random.nextInt(4);
					Integer suppKey = (partKey + (i * ((S / 4) + (partKey - 1) / S))) % S;
					Integer lineNum = j; // unique within 7, for simplicity, set to j
					Integer quantity = random.nextInt(50) + 1;

					Double extendedPrice;					
//					Double extendedPrice = (double) quantity * retailPrices[0];
					/* Null ptr */
					if(partKey < retailPrices.length) {
						extendedPrice = quantity * retailPrices[partKey];	
					}else {
						extendedPrice = quantity * retailPrices[0];
					}					

					String extendedPriceString = String.format("%.2f", extendedPrice);
					Double discount = random.nextInt(11) / 100.0;
					String discountString = String.format("%.2f", discount);
					Double tax = random.nextInt(9) / 100.0;
					String taxString = String.format("%.2f", discount);
					// set to "N" as default
					String returnFlagString = "N";
					// set to "F" as default
					String lineStatusString = "F";
					Date shipDate = addDays(orderDate, random.nextInt(121) + 1);
					Date commitDate = addDays(orderDate, random.nextInt(61) + 30);
					Date receiptDate = addDays(shipDate, random.nextInt(30) + 1);
					String shipInstructString = instructs[random.nextInt(4)];
					String shipModeString = modes[random.nextInt(7)];
					String lCommentString = random.ints(97, 122 + 1).limit(124)
							.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
							.toString();
					if (receiptDate.before(currentDate)) {
						returnFlagString = (random.nextInt(2) == 0) ? "R" : "A";
					}
					if (shipDate.after(currentDate)) {
						lineStatusString = "O";
					}
					if (lineStatusString == "O") {
						Onumber++;
					} else {
						Fnumber++;
					}
					totalPrice += extendedPrice * (1 + tax) * (1 - discount);

					this.lineItem.insertOne(new Document("order_key", L_OrderKey).append("part_key", partKey)
							.append("supp_key", suppKey).append("line_number", lineNum).append("quantity", quantity)
							.append("extended_price", extendedPriceString).append("discount", discountString)
							.append("tax", taxString).append("return_flag", returnFlagString)
							.append("line_status", lineStatusString).append("ship_date", shipDate)
							.append("commit_date", commitDate).append("receipt_date", receiptDate)
							.append("ship_instructions", shipInstructString).append("ship_mode", shipModeString)
							.append("comment", lCommentString)
							);
				}
				if (Fnumber == lineItemRows) {
					orderStatusString = "F";
				}
				if (Onumber == lineItemRows) {
					orderStatusString = "O";
				}
				String totalPriceString = String.format("%.2f", totalPrice);

				this.orders.insertOne(new Document("order_key", orderKey)
						.append("cust_key", custKey)
						.append("order_status", orderStatusString)
						.append("total_price", totalPriceString)
						.append("order_date", orderDate)
						.append("order_priority", priorityString)
						.append("clerk", clerkString)
						.append("ship_priority", shipPriorityString)
						.append("comment", oCommentString)
						);
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

			/* 200000 documents */
			System.out.println("Inserting values into part collection");
			InsertPart();
			System.out.println("Values Added to part collection");

			/* 800000 documents */
			System.out.println("Inserting values into partsupp collection");
			InsertPartSupp();
			System.out.println("Values Added to partsupp collection");

			/* 150000 documents */
			System.out.println("Inserting values into customer collection");
			InsertCustomer();
			System.out.println("Values Added to customer collection");

			/*
			 * Order - 1500000 documents lineItem - 6001215 documents
			 */
			System.out.println("insert into order and lineItem");
			InsertOrderLineItem();
			System.out.println("Values Added to order and lineItem collection");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
