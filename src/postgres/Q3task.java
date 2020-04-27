package postgres;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q3task extends QueryTask{
	
	private static final String[] Segments = {"AUTOMOBILE","BUILDING","FURNITURE","MACHINERY","HOUSEHOLD"};
	
	private static final Date baseDate = Date.valueOf("1995-03-01");
	
	private static final int dateRange = 30;

	public Q3task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String SegmentString = Segments[random.nextInt(5)];
			Date queryDate = new Date(baseDate.getTime()+(random.nextInt(dateRange)+1)*86400000);
			
			String queryString = 
					"select\n" + 
					"l_orderkey,\n" + 
					"sum(l_extendedprice*(1-l_discount)) as revenue,\n" + 
					"o_orderdate,\n" + 
					"o_shippriority\n" + 
					"from\n" + 
					"customer,\n" + 
					"orders,\n" + 
					"lineitem\n" + 
					"where\n" + 
					"c_mktsegment = '" + SegmentString + "'\n" + 
					"and c_custkey = o_custkey\n" + 
					"and l_orderkey = o_orderkey\n" + 
					"and o_orderdate < date '" + queryDate + "'\n" + 
					"and l_shipdate > date '" + queryDate + "'\n" + 
					"group by\n" + 
					"l_orderkey,\n" + 
					"o_orderdate,\n" + 
					"o_shippriority\n" + 
					"order by\n" + 
					"revenue desc,\n" + 
					"o_orderdate\n" + 
					"limit 10";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q3 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
