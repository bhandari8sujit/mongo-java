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
			
			String queryString = "select /*+ USE_SORT_MERGE_JOIN*/\n" + 
					"s_acctbal,\n" + 
					"s_name,\n" + 
					"n_name,\n" + 
					"p_partkey,\n" + 
					"p_mfgr,\n" + 
					"s_address,\n" + 
					"s_phone,\n" + 
					"s_comment\n" + 
					"from\n" + 
					"part,\n" + 
					"supplier,\n" + 
					"partsupp,\n" + 
					"nation,\n" + 
					"region\n" + 
					"where\n" + 
					"p_partkey = ps_partkey\n" + 
					"and s_suppkey = ps_suppkey\n" + 
					"and p_size = "+ sizeInteger +"\n" + 
					"and p_type like '%"+typeString+"'\n" + 
					"and s_nationkey = n_nationkey\n" + 
					"and n_regionkey = r_regionkey\n" + 
					"and r_name = '"+regionString+"'\n" + 
					"and ps_supplycost = (\n" + 
					"select\n" + 
					"min(ps_supplycost)\n" + 
					"from\n" + 
					"partsupp, supplier,\n" + 
					"nation, region\n" + 
					"where\n" + 
					"p_partkey = ps_partkey\n" + 
					"and s_suppkey = ps_suppkey\n" + 
					"and s_nationkey = n_nationkey\n" + 
					"and n_regionkey = r_regionkey\n" + 
					"and r_name = '"+regionString+"'\n" + 
					")\n" + 
					"order by\n" + 
					"s_acctbal desc,\n" + 
					"n_name,\n" + 
					"s_name,\n" + 
					"p_partkey";
			
			System.out.println("Query is:");
			System.out.println();
			System.out.println(queryString);
			System.out.println();
			
			this.startInstant = Instant.now();
			
			statement.execute(queryString);
			
			connection.commit();
			
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
