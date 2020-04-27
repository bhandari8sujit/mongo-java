//package postgres;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.Random;
//
//import org.junit.jupiter.api.Test;
//
//class test {
//
//	@Test
//	void test() {
//		Connection connection;
//		try {
//			connection = DriverManager.getConnection("jdbc:postgresql:","postgres","12345");
//			System.out.println("got connection: "+connection.toString());
//			connection.close();
//			System.out.println("closed connection");
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//}
