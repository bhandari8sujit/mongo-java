import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q7task extends QueryTask{
	
	private static final String[] Nations = {"'ALGERIA'","'ARGENTINA'","'BRAZIL'",
			"'CANADA'","'EGYPT'","'ETHIOPIA'","'FRANCE'",
			"'GERMANY'","'INDIA'","'INDONESIA'","'IRAN'",
			"'IRAQ'","'JAPAN'","'JORDAN'","'KENYA'","'MOROCCO'",
			"'MOZAMBIQUE'","'PERU'","'CHINA'","'ROMANIA'","'SAUDI ARABIA'",
			"'VIETNAM'","'RUSSIA'","'UNITED KINGDOM'","'UNITED STATES'"};

	public Q7task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String nation1 = Nations[random.nextInt(Nations.length)];
			String nation2 = Nations[random.nextInt(Nations.length)];
			while (nation1.equalsIgnoreCase(nation2)) {
				nation2 = Nations[random.nextInt(Nations.length)];
			}
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/\n" + 
					"supp_nation,\n" + 
					"cust_nation,\n" + 
					"l_year, sum(volume) as revenue\n" + 
					"from (\n" + 
					"select\n" + 
					"n1.n_name as supp_nation,\n" + 
					"n2.n_name as cust_nation,\n" + 
					"year(l_shipdate) as l_year,\n" + 
					"l_extendedprice * (1 - l_discount) as volume\n" + 
					"from\n" + 
					"supplier,\n" + 
					"lineitem,\n" + 
					"orders,\n" + 
					"customer,\n" + 
					"nation n1,\n" + 
					"nation n2\n" + 
					"where\n" + 
					"s_suppkey = l_suppkey\n" + 
					"and o_orderkey = l_orderkey\n" + 
					"and c_custkey = o_custkey\n" + 
					"and s_nationkey = n1.n_nationkey\n" + 
					"and c_nationkey = n2.n_nationkey\n" + 
					"and (\n" + 
					"(n1.n_name = "+nation1+" and n2.n_name = "+nation2+")\n" + 
					"or (n1.n_name = "+nation2+" and n2.n_name = "+nation1+")\n" + 
					")\n" + 
					"and l_shipdate >= date '1995-01-01'\n" + 
					"and l_shipdate <= date '1996-12-31'\n" + 
					") as shipping\n" + 
					"group by\n" + 
					"supp_nation,\n" + 
					"cust_nation,\n" + 
					"l_year\n" + 
					"order by\n" + 
					"supp_nation,\n" + 
					"cust_nation,\n" + 
					"l_year";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q7 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
