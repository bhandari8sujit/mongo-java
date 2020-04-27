import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Q2task extends QueryTask{
	
	private static final String[] typeSyllables3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
	
	private static final String[] RegionNames = {"AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"};

	public Q2task(Connection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeQuery() {
		// TODO Auto-generated method stub
		try {
			Statement statement = connection.createStatement();
			Random random = new Random();
			String typeString = typeSyllables3[random.nextInt(5)];
			String regionString = RegionNames[random.nextInt(5)];
			Integer sizeInteger = random.nextInt(50)+1;
			
			String queryString = "match(p:part{p_size:"+sizeInteger+"})-[ps:partsupp]->(s:supplier)"
					+ "-[:In]->(n:nation)-[:In]->(r:region{r_name:'"+regionString+"'})\n"
					+ "where p.p_type =~ '.*"+typeString+"'\n"
					+ "return p.p_partkey, min(toFloat(ps.ps_supplycost)) as minCost,\n"
					+ "s.s_acctbal,s.s_name,n.n_name,p.p_mfgr,s.s_address,"
					+ "s.s_phone,s.s_comment\n"
					+ "order by s.s_acctbal desc,"
					+ "n.n_name,s.s_name,p.p_partkey\n"
					+ "limit 100";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			//connection.commit();
			
			this.endInstant = Instant.now();
			System.out.println("Execution time for Q2 is: "+
								Duration.between(startInstant, endInstant).toMillis()+
								" miliseconds");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
