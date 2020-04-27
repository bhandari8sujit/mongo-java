import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q9task extends QueryTask{
	
	private static final String[] pNames = {"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
			"blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
			"cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
			"floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
			"hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
			"magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
			"navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
			"puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
			"sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
			"wheat", "white", "yellow"};
	
	public Q9task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String color = pNames[random.nextInt(pNames.length)];
			
			String queryString = "match(o:orders)"
					+ "-[:include]->(l:lineitem)-[:suppliedBy]->(s:supplier)"
					+ "-[:In]->(n:nation),(l)-[:contain]->(p:part),(s)<-[ps:partsupp]-(p)\n"
					+ "where p.p_name=~('.*"+color+".*')\n"
					+ "with n.n_name as nation,\n"
					+ "o.o_orderdate.year as o_year,\n"
					+ "(toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount))"
					+ "-toFloat(ps.ps_supplycost)*toFloat(l.l_quantity)) as amount\n"
					+ "return nation, o_year,sum(amount) as sum_profit\n"
					+ "order by nation, o_year";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q9 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
