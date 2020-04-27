import java.sql.Connection;
import java.sql.DriverManager;

public class initializeMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Connection connection = DriverManager.getConnection("jdbc:neo4j:bolt://localhost", "neo4j", "12345");
			System.out.println("got connection: "+connection.toString());
			
			InsertValueTask insertValueTask = new InsertValueTask(connection, (float)0.05);
			insertValueTask.InsertValues();
			
			connection.close();
			System.out.println("closed connection");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
