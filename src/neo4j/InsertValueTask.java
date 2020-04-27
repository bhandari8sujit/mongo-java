import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;


public class InsertValueTask {
	
	private Connection connection;
	
	private float sf;
	
	private Double[] retailPrices;

	public InsertValueTask(Connection connection, float sf) {
		this.connection = connection;
		this.sf = sf;
		this.retailPrices = new Double[(int) (sf*200000)];
	}
	
	private ArrayList<Integer> shuffleKey(Integer fixNum){
		Integer size = new Integer((int) (sf*fixNum));
		ArrayList<Integer> keys = new ArrayList<Integer>();
		for(int it=0 ; it < size; it++) {
			keys.add(it);
		}
		Collections.shuffle(keys);
		return keys;
	}
	
	public void InsertSupplier() {
		try {
			Statement statement = connection.createStatement();
			Integer size = (int) (sf*10000);
			ArrayList<Integer> keys = shuffleKey(10000);
			Random random = new Random();
			ArrayList<Integer> CommentList = new ArrayList<Integer>();
			Integer commentedsize = (int) (10*sf);
			Integer index = 0;
			while (CommentList.size()<commentedsize) {
				index = random.nextInt(size);
				if (!CommentList.contains(index)) {
					CommentList.add(index);
				}
			}
			for (int i = 0; i < size; i++) {
				Integer key = keys.get(i);
				String formatted_key = String.format("%09d", key);
				int length = random.nextInt(30)+10;
				String addressString = RandomStringUtils.random(length, true, true);
				Integer nationInteger = random.nextInt(25);
				String phoneString = Integer.toString(nationInteger+10)+"-"+
										Integer.toString(random.nextInt(900)+100)+"-"+
										Integer.toString(random.nextInt(900)+100)+"-"+
										Integer.toString(random.nextInt(9000)+1000);
				String acctbalString = String.format("%.2f", (random.nextInt(1099999)-99999)/100.0);
				String commentString = "No Comments";
				int commentIndex = CommentList.indexOf(key);
				if (commentIndex!=-1) {
					if (commentIndex<5*sf) {
						commentString = "Customer"+RandomStringUtils.randomAlphabetic(20)+"Complaints";
					}
					else {
						commentString = "Customer"+RandomStringUtils.randomAlphabetic(20)+"Recommands";
					}
				}
				/*
				statement.executeUpdate("UPSERT INTO SUPPLIER VALUES("+key+",'"+
											"Supplier#r"+formatted_key+"','"+
												addressString+"',"+nationInteger+",'"+
												phoneString+"',"+acctbalString+",'"+commentString+"')");*/
				statement.executeUpdate(
						"match(\n" + 
						"n:nation{\n" + 
						"n_nationkey: "+nationInteger+"\n" + 
						"})\n" + 
						"create(\n" + 
						"s:supplier{\n" + 
						"s_suppkey: "+key+"\n" + 
						",s_name: '"+"Supplier#r"+formatted_key+"'\n" + 
						",s_address: '"+addressString+"'\n" + 
						",s_phone: '"+phoneString+"'\n" + 
						",s_acctbal: "+acctbalString+"\n" + 
						",s_comment: '"+commentString+"'\n" + 
						"})\n" + 
						"create(s)-[:In]->(n)");
												
			}
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertPart() {
		
		String[] pNames = {"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
				"blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
				"cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
				"floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
				"hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
				"magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
				"navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
				"puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
				"sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
				"wheat", "white", "yellow"};
		String[] typeSyllables1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
		String[] typeSyllables2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
		String[] typeSyllables3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
		String[] containerSyllables1 = {"SM", "LG", "MED", "JUMBO", "WRAP"};
		String[] containerSyllables2 = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};
		
		try {
			Statement statement = connection.createStatement();
			Integer size = (int) (sf*200000);
			Random random = new Random();
			ArrayList<Integer> keys = shuffleKey(200000);
			
			for (int i = 0; i < size; i++) {
				Integer key = keys.get(i);
				
				ArrayList<String> selectedNames = new ArrayList<String>();
				int length = pNames.length;
				while (selectedNames.size()<5) {
					int index = random.nextInt(length);
					if (!selectedNames.contains(pNames[index])) {
						selectedNames.add(pNames[index]);
					}
				}
				String nameString = String.join(" ", selectedNames);
				
				int M = random.nextInt(5)+1;
				
				String mfgrString = "Manufacturer#"+M;
				
				String brandString = "Brand#"+String.valueOf(M)+String.valueOf((random.nextInt(5)+1));
				
				String typeString = typeSyllables1[random.nextInt(6)]+" "+
									typeSyllables2[random.nextInt(5)]+" "+
									typeSyllables3[random.nextInt(5)];
				
				String sizeString = String.valueOf(random.nextInt(50)+1);
				
				String containerString = containerSyllables1[random.nextInt(5)]+" "+
											containerSyllables2[random.nextInt(8)];
				
				Double price = (90000+(key/10)%20001+100*key%1000)/100.0;
				retailPrices[key] = price;
				String retailPriceString = String.format("%.2f", price);
				
				String pCommentString = RandomStringUtils.random(5+random.nextInt(18), true, true);
				
				/*statement.executeUpdate("UPSERT INTO PART VALUES("+key+",'"+nameString+"','"+mfgrString+"','"+brandString+
										"','"+typeString+"',"+sizeString+",'"+containerString+"',"+retailPriceString+",'"+
										pCommentString+"')");*/
				statement.executeUpdate(
						"create(\n" + 
						":part{\n" + 
						"p_partkey: "+key+"\n" + 
						",p_name: '"+nameString+"'\n" + 
						",p_mfgr: '"+mfgrString+"'\n" + 
						",p_brand: '"+brandString+"'\n" + 
						",p_type: '"+typeString+"'\n" + 
						",p_size: "+sizeString+"\n" + 
						",p_container: '"+containerString+"'\n" + 
						",p_retailprice: "+retailPriceString+"\n" + 
						",p_comment: '"+pCommentString+"'\n" + 
						"})");
				//if (i%9000==0) {
					//connection.commit();
				//}
			}
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertPartSupp() {
		try {
			Statement statement = connection.createStatement();
			Integer S = (int) (sf*10000);
			Integer size = (int) (sf*200000);
			Random random = new Random();
			ArrayList<Integer> psKeys = shuffleKey(200000);
			for (int idx = 0; idx < size; idx++) {
				Integer psKey = psKeys.get(idx);
				for(int i = 0; i < 4; i++) {
					Integer psSuppKey = (psKey+(i*((S/4)+(psKey-1)/S)))%S;
					Integer psAvailQTY = random.nextInt(9999)+1;
					String psSupplyCost = String.format("%.2f", (random.nextInt(99901)+100)/100.0);
					int length = random.nextInt(150)+49;
					String psComment = RandomStringUtils.random(length, true, true);
					/*statement.executeUpdate("UPSERT INTO PARTSUPP VALUES("+psKey+","+
											psSuppKey+","+psAvailQTY+","+psSupplyCost+",'"+psComment+"')");*/
					statement.executeUpdate(
							"match(p:part{p_partkey: "+psKey+"\n" + 
							"})\n" + 
							"match(s:supplier{s_suppkey: "+psSuppKey+"\n" + 
							"})\n" + 
							"create(p)-[partsupp:partsupp{\n" + 
							"ps_availqty: "+psAvailQTY+"\n" + 
							",ps_supplycost: "+psSupplyCost+"\n" + 
							",ps_comment: '"+psComment+"'\n" + 
							"}]->(s)");
				}
				//if (idx%2000==0) {
					//connection.commit();
				//}
			}
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void InsertCustomer() {
		
		String[] Segments = {"AUTOMOBILE","BUILDING","FURNITURE","MACHINERY","HOUSEHOLD"};
		
		try {
			Statement statement = connection.createStatement();
			Integer size = (int) (sf*150000);
			Random random = new Random();
			ArrayList<Integer> keys = shuffleKey(150000);
			for (int i = 0; i < size; i++) {
				Integer key = keys.get(i);
				String cNameString = "Customer#"+String.format("%09d", key);
				int length = random.nextInt(30)+10;
				String addressString = RandomStringUtils.random(length, true, true);
				Integer nationInteger = random.nextInt(25);
				String phoneString = Integer.toString(nationInteger+10)+"-"+
										Integer.toString(random.nextInt(900)+100)+"-"+
										Integer.toString(random.nextInt(900)+100)+"-"+
										Integer.toString(random.nextInt(9000)+1000);
				String acctbalString = String.format("%.2f", (random.nextInt(1099999)-99999)/100.0 );
				String mktSegmentString = Segments[random.nextInt(5)];
				Integer count = random.nextInt(88)+29;
				String commentString = RandomStringUtils.randomAlphabetic(count);
				/*statement.executeUpdate("UPSERT INTO CUSTOMER VALUES("+key+",'"+cNameString+"','"+addressString+"',"+
										nationInteger+",'"+phoneString+"',"+acctbalString+",'"+mktSegmentString+"','"+
										commentString+"')");*/
				statement.executeUpdate(
						"match(\n" + 
						"n:nation{\n" + 
						"n_nationkey: "+nationInteger+"\n" + 
						"})\n" + 
						"create(\n" + 
						"c:customer{\n" + 
						"c_custkey: "+key+"\n" + 
						",c_name: '"+cNameString+"'\n" + 
						",c_address: '"+addressString+"'\n" + 
						",c_phone: '"+phoneString+"'\n" + 
						",c_acctbal: "+acctbalString+"\n" + 
						",c_mktsegment: '"+mktSegmentString+"'\n" + 
						",c_comment: '"+commentString+"'\n" + 
						"})\n" + 
						"create (c)-[:In]->(n)");
				//if (i%9000==0) {
					//connection.commit();
				//}
			}
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertOrderLineItem() {
		
		String[] priorities = { "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW" };
		String[] instructs = { "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN" };
		String[] modes = { "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB" };
		
		LocalDate startDate = LocalDate.parse("1992-01-01");
		LocalDate currentDate = LocalDate.parse("1995-06-17");
		LocalDate endDate = LocalDate.parse("1998-12-31");
		
		int dayRange = 2405;
		
		try {
			Statement statement = connection.createStatement();
			Integer orderSize = (int) (sf * 1500000 * 4);
			ArrayList<Integer> orderKeys = shuffleKey(1500000 * 4);
			Random random = new Random();
			for (int idx = 0; idx < orderSize; idx++) {
				Integer orderKey = orderKeys.get(idx);
				if (orderKey%4!=0) {
					continue;
				}
				Integer custKey = 0;
				do {
					custKey = random.nextInt((int) (sf*150000));
				} while (custKey%3==0);
				// set to P as default, will change according to lineItems
				String orderStatusString = "P";
				// set to 0.0 as place holder, will change according to lineItems
				Double totalPrice = 0.0;
				
				LocalDate orderDate = startDate.plusDays(random.nextInt(dayRange));
				
				String priorityString = priorities[random.nextInt(5)];
				String clerkString = "Clerk#"+String.format("%09d", random.nextInt((int) (sf*1000))+1);
				String shipPriorityString = "0";
				String oCommentString = RandomStringUtils.randomAlphabetic( random.nextInt(60)+18 );
				// Insert incomplete orders node
				statement.executeUpdate(
						"match(c:customer{c_custkey: "+custKey+"\n" + 
						"})\n" + 
						"create(o:orders{o_orderkey: "+orderKey+"\n" + 
						",o_orderdate: date('"+orderDate+"')\n" + 
						",o_orderpriority: '"+priorityString+"'\n" + 
						",o_clerk: '"+clerkString+"'\n" + 
						",o_shippriority: "+shipPriorityString+"\n" + 
						",o_comment: '"+oCommentString+"'\n" + 
						"})\n" + 
						"create (c)<-[:from]-(o)");
				//connection.commit();
				
				int lineItemRows = random.nextInt(7)+1;
				int Fnumber = 0;
				int Onumber = 0;
				for(int j = 0; j < lineItemRows; j++) {
					Integer L_OrderKey = orderKey;
					Integer partKey = random.nextInt((int) (sf*200000));
					int S = (int) (sf*10000);
					int i = random.nextInt(4);
					Integer suppKey = (partKey+(i*((S/4)+(partKey-1)/S)))%S;
					Integer lineNum = j; // unique within 7, for simplicity, set to j
					Integer quantity = random.nextInt(50)+1;
					Double extendedPrice = quantity * retailPrices[partKey];
					//ResultSet rs = statement.executeQuery(
					//		"match(p:part{p_partkey: "+partKey+"})return p.p_retailprice"
					//		);
					//Double retail_price = 0.0;
					//while (rs.next()) {
					//	retail_price = rs.getObject("p.p_retailprice", Double.class);
					//}
					//Double extendedPrice = quantity * retail_price;
					String extendedPriceString = String.format("%.2f", extendedPrice);
					Double discount = random.nextInt(11)/100.0;
					String discountString = String.format("%.2f", discount);
					Double tax = random.nextInt(9)/100.0;
					String taxString = String.format("%.2f", discount);
					// set to "N" as default
 					String returnFlagString = "N";
					// set to "F" as default
					String lineStatusString = "F";
					
					LocalDate shipDate = orderDate.plusDays(random.nextInt(121)+1);
					LocalDate commitDate = orderDate.plusDays(random.nextInt(61)+30);
					LocalDate receiptDate = shipDate.plusDays(random.nextInt(30)+1);
					
					String shipInstructString = instructs[random.nextInt(4)];
					String shipModeString = modes[random.nextInt(7)];
					String lCommentString = RandomStringUtils.randomAlphabetic(random.nextInt(34)+10);
					
					if (receiptDate.isBefore(currentDate)) {
						returnFlagString = (random.nextInt(2)==0)?"R":"A";
					}
					if (shipDate.isAfter(currentDate)) {
						lineStatusString = "O";
					}
					if (lineStatusString.equalsIgnoreCase("O")) {
						Onumber++;
					}
					else {
						Fnumber++;
					}
					totalPrice+=extendedPrice*(1+tax)*(1-discount);
					/*statement.executeUpdate("UPSERT INTO LINEITEM VALUES("+L_OrderKey+","+partKey+","+suppKey+","+
											lineNum+","+quantity+","+extendedPriceString+","+discountString+","+
											taxString+",'"+returnFlagString+"','"+lineStatusString+"','"+
											shipDate+"','"+commitDate+"','"+receiptDate+"','"+
											shipInstructString+"','"+shipModeString+"','"+lCommentString+"')");*/
					statement.executeUpdate(
							"match(p:part{p_partkey: "+partKey+"\n" + 
							"})\n" + 
							"match(s:supplier{s_suppkey: "+suppKey+"\n" + 
							"})\n" + 
							"match(o:orders{o_orderkey: "+L_OrderKey+"\n" + 
							"})\n" + 
							"create(\n" + 
							"l:lineitem{\n" + 
							"l_linenumber: "+lineNum+"\n" + 
							",l_quantity: "+quantity+"\n" + 
							",l_extendedprice: "+extendedPriceString+"\n" + 
							",l_discount: "+discountString+"\n" + 
							",l_tax: "+taxString+"\n" + 
							",l_returnflag: '"+returnFlagString+"'\n" + 
							",l_linestatus: '"+lineStatusString+"'\n" + 
							",l_shipdate: date('"+shipDate+"')\n" + 
							",l_commitdate: date('"+commitDate+"')\n" + 
							",l_receiptdate: date('"+receiptDate+"')\n" + 
							",l_shipinstruct: '"+shipInstructString+"'\n" + 
							",l_shipmode: '"+shipModeString+"'\n" + 
							",l_comment: '"+lCommentString+"'\n" + 
							"})\n" + 
							"create (l)-[:contain]->(p)\n" + 
							"create (l)-[:suppliedBy]->(s)\n"+
							"create (o)-[:include]->(l)");
				}
				if (Fnumber==lineItemRows) {
					orderStatusString = "F";
				}
				if (Onumber==lineItemRows) {
					orderStatusString = "O";
				}
				String totalPriceString = String.format("%.2f", totalPrice);
				/*statement.executeUpdate("UPSERT INTO ORDERS VALUES("+orderKey+","+custKey+",'"+orderStatusString+"',"+
										totalPriceString+",'"+orderDate+"','"+priorityString+"','"+clerkString+"',"+
										shipPriorityString+",'"+oCommentString+"')");*/
				
				// Set properties cannot be determined before lineItems
				statement.executeUpdate(
						"match(o:orders{o_orderkey:"+orderKey+"\n" + 
						"})\n" + 
						"set\n" + 
						"o.o_orderstatus= '"+orderStatusString+"'\n" + 
						",o.o_totalprice= "+totalPriceString);
				//connection.commit();
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertNation() {
		String[] nationNames = {"'ALGERIA'","'ARGENTINA'","'BRAZIL'",
				"'CANADA'","'EGYPT'","'ETHIOPIA'","'FRANCE'",
				"'GERMANY'","'INDIA'","'INDONESIA'","'IRAN'",
				"'IRAQ'","'JAPAN'","'JORDAN'","'KENYA'","'MOROCCO'",
				"'MOZAMBIQUE'","'PERU'","'CHINA'","'ROMANIA'","'SAUDI ARABIA'",
				"'VIETNAM'","'RUSSIA'","'UNITED KINGDOM'","'UNITED STATES'"};
		int[] RegionKeys = {0,1,1,1,4,0,3,3,2,2,4,4,2,4,0,0,0,1,2,3,4,2,3,3,1};
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			for (int i = 0; i < nationNames.length; i++) {
				String commentString = RandomStringUtils.randomAlphabetic(random.nextInt(84)+31);
				/*statement.executeUpdate("UPSERT INTO NATION VALUES("+i+","+
										nationNamesRegionKeys[i]+",'"+commentString+"')");*/
				statement.executeUpdate(
						"match(r:region{\n" + 
						"r_regionkey: "+RegionKeys[i]+"\n" + 
						"})\n" +
						"create(n:nation{\n" + 
						"n_nationkey: "+i+"\n" + 
						",n_name: "+nationNames[i]+"\n" + 
						",n_comment: '"+commentString+"'\n" + 
						"})\n" + 
						"create (n)-[:In]->(r)");
			}
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void InsertRegion() {
		String[] RegionNames = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			for (int i = 0; i < RegionNames.length; i++) {
				String commentString = RandomStringUtils.randomAlphabetic(random.nextInt(85)+31);
				/*statement.executeUpdate("UPSERT INTO REGION VALUES("+i+",'"+
										RegionNames[i]+"','"+commentString+"')");*/
				statement.executeUpdate(
						"create(:region{\n" + 
						"r_regionkey: "+i+"\n" + 
						",r_name: '"+RegionNames[i]+"'\n" + 
						",r_comment: '"+commentString+"'\n" + 
						"})");
			}
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertValues() {
		try {		
			// The order should be considered carefully
			System.out.println("insert into region");
			InsertRegion();
			System.out.println("insert into nation");
			InsertNation();
			System.out.println("insert into supplier");
			InsertSupplier();
			System.out.println("insert into part");
			InsertPart();
			System.out.println("insert into partsupp");
			InsertPartSupp();
			System.out.println("insert into customer");
			InsertCustomer();
			System.out.println("insert into order and lineItem");
			InsertOrderLineItem();
			//cannot commit when in autocommit
			//connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
