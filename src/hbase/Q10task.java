
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.util.Random;

public class Q10task extends QueryTask{
	
	private static final LocalDate baseDate = LocalDate.parse("1993-02-01");
	
	public Q10task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			Period period = baseDate.until(LocalDate.parse("1995-01-01"));
			int range = period.getMonths()+period.getYears()*12;
			LocalDate queryDate1 = baseDate.plusMonths(random.nextInt(range+1));
			LocalDate queryDate2 = queryDate1.plusMonths(3);
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/\n" + 
					"c_custkey,\n" + 
					"c_name,\n" + 
					"sum(l_extendedprice * (1 - l_discount)) as revenue,\n" + 
					"c_acctbal,\n" + 
					"n_name,\n" + 
					"c_address,\n" + 
					"c_phone,\n" + 
					"c_comment\n" + 
					"from\n" + 
					"customer,\n" + 
					"orders,\n" + 
					"lineitem,\n" + 
					"nation\n" + 
					"where\n" + 
					"c_custkey = o_custkey\n" + 
					"and l_orderkey = o_orderkey\n" + 
					"and o_orderdate >= date '"+queryDate1+"'\n" + 
					"and o_orderdate < date '"+queryDate2+"'\n" + 
					"and l_returnflag = 'R'\n" + 
					"and c_nationkey = n_nationkey\n" + 
					"group by\n" + 
					"c_custkey,\n" + 
					"c_name,\n" + 
					"c_acctbal,\n" + 
					"c_phone,\n" + 
					"n_name,\n" + 
					"c_address,\n" + 
					"c_comment\n" + 
					"order by\n" + 
					"revenue desc\n" + 
					"limit 20";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q10 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
