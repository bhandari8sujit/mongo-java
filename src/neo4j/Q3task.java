import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Random;

public class Q3task extends QueryTask{
	
	private static final String[] Segments = {"AUTOMOBILE","BUILDING","FURNITURE","MACHINERY","HOUSEHOLD"};
	
	private static final LocalDate baseDate = LocalDate.parse("1995-03-01");
	
	private static final int dateRange = 31;

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
			LocalDate queryDate = baseDate.plusDays(random.nextInt(dateRange));
			
			String queryString = "match(l:lineitem)<-[:include]-(o:orders)-[:from]->(c:customer)"
					+ "where c.c_mktsegment='"+SegmentString+"'\n"
					+ "and o.o_orderdate<date('"+queryDate+"')\n"
					+ "and l.l_shipdate>date('"+queryDate+"')\n"
					+ "return o.o_orderkey,o.o_orderdate,o.o_shippriority,"
					+ "sum(toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount))) as revenue\n"
					+ "order by revenue desc,o.o_orderdate";
			
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
