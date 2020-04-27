package postgres;

import java.sql.Connection;
import java.sql.DriverManager;

public class InitializeMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// final String url = "jdbc:postgresql://localhost:56212";
		try {
			Connection connection = DriverManager.getConnection("jdbc:postgresql:","sujit","12345");
			System.out.println("got connection: "+connection.toString());
			CreateTableTask taskCreateTable = new CreateTableTask(connection);
			taskCreateTable.CreateTables();
			InsertValueTask taskInsertValue = new InsertValueTask(connection, (float) 0.05);
			taskInsertValue.InsertValues();
			connection.close();
			System.out.println("closed connection");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
