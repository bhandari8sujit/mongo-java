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
			
			String queryString = "match(c:customer)<-[:from]-(o:orders)-[:include]->(l:lineitem)"
					+ "-[:suppliedBy]->(s:supplier)-[:In]->(n:nation)-[:In]->(r:region)\n"
					+ "where r.r_name='"+ regionString +"'\n"
					+ "and o.o_orderdate>=date('"+queryDate+"')\n"
					+ "and o.o_orderdate<date('"+queryDate.plusYears(1)+"')\n"
					+ "with n.n_name as n_name, sum(toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount))) as revenue\n"
					+ "return n_name, revenue\n"
					+ "order by revenue desc";
			
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