import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.util.Random;

public class Q4task extends QueryTask{
	
	private static final LocalDate baseDate = LocalDate.parse("1993-01-01");

	public Q4task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			Period dateRange = baseDate.until(LocalDate.parse("1997-10-01"));
			int range = dateRange.getMonths()+dateRange.getYears()*12;
			LocalDate queryDate = baseDate.plusMonths(random.nextInt(range+1));
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/ \n" + 
					"o_orderpriority,\n" + 
					"count(*) as order_count\n" + 
					"from\n" + 
					"orders\n" + 
					"where\n" + 
					"o_orderdate >= date '"+queryDate+"'\n" + 
					"and o_orderdate < date '"+queryDate.plusMonths(3)+"'\n" + 
					"and exists (\n" + 
					"select\n" + 
					"*\n" + 
					"from\n" + 
					"lineitem\n" + 
					"where\n" + 
					"l_orderkey = o_orderkey\n" + 
					"and l_commitdate < l_receiptdate\n" + 
					")\n" + 
					"group by\n" + 
					"o_orderpriority\n" + 
					"order by\n" + 
					"o_orderpriority";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q4 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
