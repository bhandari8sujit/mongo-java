import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.jupiter.api.Test;

class test {

	@Test
	void test() throws SQLException {
		Connection connection = DriverManager.getConnection("jdbc:neo4j:bolt://localhost", "neo4j", "12345");
		System.out.println("got connection: "+connection.toString());
		Random random = new Random();
		Statement statement = connection.createStatement();
		for(int i=0;i<200000;i++) {
			ResultSet rs = statement.executeQuery(
					"match(p:part{p_partkey: "+i+"})return p.p_retailprice"
					);
			while (rs.next()) {
				System.out.println(i+"   "+rs.getObject("p.p_retailprice", Double.class));
			}
		}
		connection.close();
	}

}
