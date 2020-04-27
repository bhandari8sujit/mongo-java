import java.sql.Connection;
import java.sql.DriverManager;

public class InitializeMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			Connection connection = DriverManager.getConnection("jdbc:phoenix:");
			System.out.println("got connection: "+connection.toString());
			CreateTableTask taskCreateTable = new CreateTableTask(connection);
			taskCreateTable.CreateTables();
			InsertValueTask taskInsertValue = new InsertValueTask(connection, (float) 0.05);
			taskInsertValue.InsertValues();
			connection.close();
			System.out.println("closed connection");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
