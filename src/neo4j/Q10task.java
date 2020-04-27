
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.util.Random;

import javax.naming.spi.DirStateFactory.Result;

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
			
			String queryString = "match(n:nation)<-[:In]-(c:customer)<-[:from]-(o:orders)"
					+ "-[:include]->(l:lineitem)\n"
					+ "where o.o_orderdate>=date('"+queryDate1+"')\n"
					+ "and o.o_orderdate<date('"+queryDate2+"')\n"
					+ "and l.l_returnflag='R'\n"
					+ "return c.c_name, c.c_acctbal, c.c_phone,\n"
					+ "n.n_name, c.c_address, c.c_comment,\n"
					+ "sum(toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount))) as revenue\n"
					+ "order by revenue desc\n"
					+ "limit 20";
			
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
