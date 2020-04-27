import java.sql.Connection;
import java.time.Instant;

public abstract class QueryTask {
	
	protected Connection connection;
	
	protected Instant startInstant;
	
	protected Instant endInstant;
	
	public QueryTask(Connection connection) {
		// TODO Auto-generated constructor stub
		this.connection = connection;
	}
	
	abstract public void executeQuery();
	
}
