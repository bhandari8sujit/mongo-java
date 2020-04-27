import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Random;

public class Q1task extends QueryTask{

	private static final LocalDate baseDate = LocalDate.parse("1998-12-01");
	
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
			LocalDate queryDate = baseDate.plusDays(-delta);
			
			String queryString = "match(l:lineitem)\n"+
					"where l.l_shipdate<=date('"+queryDate+"')\n"+
					"return l.l_returnflag as returnflag, l.linestatus as linestatus,"
					+ " sum(toFloat(l.l_quantity)) as sum_qty,"
					+ " sum(toFloat(l.l_extendedprice)) as sum_base_price,"
					+ " sum(toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount))) as sum_disc_price,"
					+ " sum(toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount))*(1+toFloat(l.l_tax))) as sum_charge,"
					+ " avg(toFloat(l.l_quantity)) as avg_qty,"
					+ " avg(toFloat(l.l_extendedprice)) as avg_price,"
					+ " avg(toFloat(l.l_discount)) as avg_disc,"
					+ " count(l) as count_order\n"
					+ "order by returnflag, linestatus";
			
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
