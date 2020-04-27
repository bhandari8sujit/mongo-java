import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q1task extends QueryTask{

	private static final Date baseDate = Date.valueOf("1998-12-01");
	
	public Q1task(Connection connection) {
		super(connection);
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			int delta = random.nextInt(61)+60;
			Date queryDate = new Date(baseDate.getTime()-delta*86400000);
			
			String queryString = "select\n" + 
					"l_returnflag,\n" + 
					"l_linestatus,\n" + 
					"sum(l_quantity) as sum_qty,\n" + 
					"sum(l_extendedprice) as sum_base_price,\n" + 
					"sum(l_extendedprice*(1-l_discount)) as sum_disc_price,\n" + 
					"sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,\n" + 
					"avg(l_quantity) as avg_qty,\n" + 
					"avg(l_extendedprice) as avg_price,\n" + 
					"avg(l_discount) as avg_disc,\n" + 
					"count(*) as count_order\n" + 
					"from\n" + 
					"lineitem\n" + 
					"where\n" + 
					"l_shipdate <= date '"+queryDate+"'\n" + 
					"group by\n" + 
					"l_returnflag,\n" + 
					"l_linestatus\n" + 
					"order by\n" + 
					"l_returnflag,\n" + 
					"l_linestatus";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q1 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}
