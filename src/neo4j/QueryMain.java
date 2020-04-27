import java.sql.Connection;
import java.sql.DriverManager;

public class QueryMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Connection connection = DriverManager.getConnection("jdbc:neo4j:bolt://localhost", "neo4j", "12345");
			System.out.println("got connection: "+connection.toString());
			
			//Q1task q1task = new Q1task(connection);
			//q1task.executeQuery();
			//Q2task q2task = new Q2task(connection);
			//q2task.executeQuery();
			//Q3task q3task = new Q3task(connection);
			//q3task.executeQuery();
			//Q4task q4task = new Q4task(connection);
			//q4task.executeQuery();
			//Q5task q5task = new Q5task(connection);
			//q5task.executeQuery();
			//Q6task q6task = new Q6task(connection);
			//q6task.executeQuery();
			//Q7task q7task = new Q7task(connection);
			//q7task.executeQuery();
			//Q8task q8task = new Q8task(connection);
			//q8task.executeQuery();
			//Q9task q9task = new Q9task(connection);
			//q9task.executeQuery();
			//Q10task q10task = new Q10task(connection);
			//q10task.executeQuery();
			connection.close();
			System.out.println("closed connection");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
