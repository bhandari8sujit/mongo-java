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
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/\n" + 
					"nation,\n" + 
					"o_year,\n" + 
					"sum(amount) as sum_profit\n" + 
					"from (\n" + 
					"select\n" + 
					"n_name as nation,\n" + 
					"year(o_orderdate) as o_year,\n" + 
					"l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n" + 
					"from\n" + 
					"part,\n" + 
					"supplier,\n" + 
					"lineitem,\n" + 
					"partsupp,\n" + 
					"orders,\n" + 
					"nation\n" + 
					"where\n" + 
					"s_suppkey = l_suppkey\n" + 
					"and ps_suppkey = l_suppkey\n" + 
					"and ps_partkey = l_partkey\n" + 
					"and p_partkey = l_partkey\n" + 
					"and o_orderkey = l_orderkey\n" + 
					"and s_nationkey = n_nationkey\n" + 
					"and p_name like '%"+color+"%'\n" + 
					") as profit\n" + 
					"group by\n" + 
					"nation,\n" + 
					"o_year\n" + 
					"order by\n" + 
					"nation,\n" + 
					"o_year desc";
			
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
