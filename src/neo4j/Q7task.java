import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q7task extends QueryTask{
	
	private static final String[] Nations = {"'ALGERIA'","'ARGENTINA'","'BRAZIL'",
			"'CANADA'","'EGYPT'","'ETHIOPIA'","'FRANCE'",
			"'GERMANY'","'INDIA'","'INDONESIA'","'IRAN'",
			"'IRAQ'","'JAPAN'","'JORDAN'","'KENYA'","'MOROCCO'",
			"'MOZAMBIQUE'","'PERU'","'CHINA'","'ROMANIA'","'SAUDI ARABIA'",
			"'VIETNAM'","'RUSSIA'","'UNITED KINGDOM'","'UNITED STATES'"};

	public Q7task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String nation1 = Nations[random.nextInt(Nations.length)];
			String nation2 = Nations[random.nextInt(Nations.length)];
			while (nation1.equalsIgnoreCase(nation2)) {
				nation2 = Nations[random.nextInt(Nations.length)];
			}
			
			String queryString = "match(n2:nation)<-[:In]-(c:customer)<-[:from]-(o:orders)"
					+ "-[:include]->(l:lineitem)-[:suppliedBy]->(s:supplier)"
					+ "-[:In]->(n1:nation)\n"
					+ "where (n1.n_name="+nation1+" and n2.n_name="+nation2+")\n"
					+ "or (n1.n_name="+nation2+" and n2.n_name="+nation1+")\n"
					+ "with n1.n_name as supp_nation, n2.n_name as cust_nation,\n"
					+ "l.l_shipdate.year as l_year,\n"
					+ "toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount)) as volume\n"
					+ "return supp_nation,cust_nation,l_year,sum(volume) as revenue\n"
					+ "order by supp_nation,cust_nation,l_year";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q7 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
