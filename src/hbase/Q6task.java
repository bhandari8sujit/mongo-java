import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Random;

public class Q6task extends QueryTask{

    private static final LocalDate baseDate = LocalDate.parse("1993-01-01");
	
	private static final int dateRange = 4;
	
	private static final int discountRange = 8;
	
	private static final int quatityRange = 3;
	
	public Q6task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			LocalDate queryDate = baseDate.plusYears(random.nextInt(dateRange+1));
			LocalDate queryDate2 = queryDate.plusYears(1);
			double discount = (float)(random.nextInt(discountRange)) / 100.0 + 0.02;
			String discountUpper = String.format("%.2f", discount+0.01);
			String discountLower = String.format("%.2f", discount-0.01);
			int quantity = random.nextInt(quatityRange)+24;
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/\n" + 
					"sum(l_extendedprice*l_discount) as revenue\n" + 
					"from\n" + 
					"lineitem\n" + 
					"where\n" + 
					"l_shipdate >= date '"+queryDate+"'\n" + 
					"and l_shipdate < date '"+queryDate2+"'\n" + 
					"and l_discount >=" + discountLower + "\n" +
					"and l_discount <=" + discountUpper + "\n" +
					"and l_quantity < "+quantity;
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q6 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
