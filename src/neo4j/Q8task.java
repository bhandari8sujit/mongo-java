import java.sql.Connection;
import java.sql.Date;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q8task extends QueryTask{
	
	private static final String[] Nations = {"'ALGERIA'","'ARGENTINA'","'BRAZIL'",
			"'CANADA'","'EGYPT'","'ETHIOPIA'","'FRANCE'",
			"'GERMANY'","'INDIA'","'INDONESIA'","'IRAN'",
			"'IRAQ'","'JAPAN'","'JORDAN'","'KENYA'","'MOROCCO'",
			"'MOZAMBIQUE'","'PERU'","'CHINA'","'ROMANIA'","'SAUDI ARABIA'",
			"'VIETNAM'","'RUSSIA'","'UNITED KINGDOM'","'UNITED STATES'"};
	
	private static final String[] typeSyllables1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
	private static final String[] typeSyllables2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
	private static final String[] typeSyllables3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
	
	private static final String[] RegionNames = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};
	
	public Q8task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String nation = Nations[random.nextInt(Nations.length)];
			String region = RegionNames[random.nextInt(RegionNames.length)];
			String type = typeSyllables1[random.nextInt(6)]+" "+
					typeSyllables2[random.nextInt(5)]+" "+
					typeSyllables3[random.nextInt(5)];
			
			String queryString = "match(r:region)<-[:In]-(n1:nation)"
					+ "<-[:In]-(c:customer)<-[:from]-(o:orders)"
					+ "-[:include]->(l:lineitem)-[:suppliedBy]->(s:supplier)"
					+ "-[:In]->(n2:nation),(l)-[:contain]->(p:part)\n"
					+ "where r.r_name='"+region+"'\n"
					+ "and o.o_orderdate>=date('1995-01-01')\n"
					+ "and o.o_orderdate<=date('1996-12-31')\n"
					+ "and p.p_type='"+type+"'\n"
					+ "with o.o_orderdate.year as o_year,"
					+ "toFloat(l.l_extendedprice)*(1-toFloat(l.l_discount)) as volume,\n"
					+ "n2.n_name as nation\n"
					+ "return o_year,sum(case"
					+ " when nation="+nation+"\n"
					+ " then volume\n"
					+ " else 0\n"
					+ " end)/sum(volume) as mkt_share\n"
					+ "order by o_year";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q8 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
