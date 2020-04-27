import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q8task extends QueryTask{
	
	private static final String[] Nations = {"'ALGERIA'","'ARGENTINA'","'BRAZIL'",
			"'CANADA'","'EGYPT'","'ETHIOPIA'","'FRANCE'",
			"'GERMANY'","'INDIA'","'INDONESIA'","'IRAN'",
			"'IRAQ'","'JAPAN'","'JORDAN'","'KENYA'","'MOROCCO'",
			"'MOZAMBIQUE'","'PERU'","'CHINA'","'ROMANIA'","'SAUDI ARABIA'",
			"'VIETNAM'","'RUSSIA'","'UNITED KINGDOM'","'UNITED STATES'"};
	
	private static final String[] typeSyllables1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
	private static final String[] typeSyllables2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
	private static final String[] typeSyllables3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
	
	private static final String[] RegionNames = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};
	
	public Q8task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String nation = Nations[random.nextInt(Nations.length)];
			String region = RegionNames[random.nextInt(RegionNames.length)];
			String type = typeSyllables1[random.nextInt(6)]+" "+
					typeSyllables2[random.nextInt(5)]+" "+
					typeSyllables3[random.nextInt(5)];
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/\n" + 
					"o_year,\n" + 
					"sum(case\n" + 
					"when nation = "+nation+"\n" + 
					"then volume\n" + 
					"else 0\n" + 
					"end) / sum(volume) as mkt_share\n" + 
					"from (\n" + 
					"select\n" + 
					"year(o_orderdate) as o_year,\n" + 
					"l_extendedprice * (1-l_discount) as volume,\n" + 
					"n2.n_name as nation\n" + 
					"from\n" + 
					"part,\n" + 
					"supplier,\n" + 
					"lineitem,\n" + 
					"orders,\n" + 
					"customer,\n" + 
					"nation n1,\n" + 
					"nation n2,\n" + 
					"region\n" + 
					"where\n" + 
					"p_partkey = l_partkey\n" + 
					"and s_suppkey = l_suppkey\n" + 
					"and l_orderkey = o_orderkey\n" + 
					"and o_custkey = c_custkey\n" + 
					"and c_nationkey = n1.n_nationkey\n" + 
					"and n1.n_regionkey = r_regionkey\n" + 
					"and r_name = '"+region+"'\n" + 
					"and s_nationkey = n2.n_nationkey\n" + 
					"and o_orderdate >= date '1995-01-01' and o_orderdate <= date '1996-12-31'\n" + 
					"and p_type = '"+type+"'\n" + 
					") as all_nations\n" + 
					"group by\n" + 
					"o_year\n" + 
					"order by\n" + 
					"o_year";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q8 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
