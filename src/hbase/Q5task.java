import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.util.Random;

public class Q5task extends QueryTask{
	
	private static final LocalDate baseDate = LocalDate.parse("1993-01-01");
	
	private static final String[] RegionNames = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};

	public Q5task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			Period dateRange = baseDate.until(LocalDate.parse("1997-01-01"));
			int range = dateRange.getYears()+1;
			LocalDate queryDate = baseDate.plusYears(random.nextInt(range));
			String regionString = RegionNames[random.nextInt(5)];
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/ \n" + 
					"n_name,\n" + 
					"sum(l_extendedprice * (1 - l_discount)) as revenue\n" + 
					"from\n" + 
					"customer,\n" + 
					"orders,\n" + 
					"lineitem,\n" + 
					"supplier,\n" + 
					"nation,\n" + 
					"region\n" + 
					"where\n" + 
					"c_custkey = o_custkey\n" + 
					"and l_orderkey = o_orderkey\n" + 
					"and l_suppkey = s_suppkey\n" + 
					"and c_nationkey = s_nationkey\n" + 
					"and s_nationkey = n_nationkey\n" + 
					"and n_regionkey = r_regionkey\n" + 
					"and r_name = '"+regionString+"'\n" + 
					"and o_orderdate >= date '"+queryDate+"'\n" + 
					"and o_orderdate < date '"+queryDate.plusYears(1)+"'\n" + 
					"group by\n" + 
					"n_name\n" + 
					"order by\n" + 
					"revenue desc";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q5 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}