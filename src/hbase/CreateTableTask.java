import java.sql.Connection;
import java.sql.Statement;

public class CreateTableTask {
	
	private Connection connection;

	public CreateTableTask(Connection connection) {
		this.connection = connection;
	}
	
	static String[] tableNames = {
			"PART",
			"SUPPLIER",
			"PARTSUPP",
			"CUSTOMER",
			"ORDERS",
			"LINEITEM",
			"NATION",
			"REGION"
	};
	
	public void CreateTables() {
		try {
			Statement statement = connection.createStatement();
			for (String tableName : tableNames) {
				statement.executeUpdate("DROP TABLE IF EXISTS "+ tableName);
			}
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS PART(\n" + 
					"P_PARTKEY BIGINT NOT NULL PRIMARY KEY,\n" + 
					"P_NAME VARCHAR(55),\n" + 
					"P_MFGR CHAR(25),\n" + 
					"P_BRAND CHAR(10),\n" + 
					"P_TYPE VARCHAR(25),\n" + 
					"P_SIZE INTEGER,\n" + 
					"P_CONTAINER CHAR(10),\n" + 
					"P_RETAILPRICE DECIMAL(12,2),\n" + 
					"P_COMMENT VARCHAR(23)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS SUPPLIER(\n" + 
					"S_SUPPKEY BIGINT NOT NULL PRIMARY KEY,\n" + 
					"S_NAME CHAR(25),\n" + 
					"S_ADDRESS VARCHAR(40),\n" + 
					"S_NATIONKEY INTEGER,\n" + 
					"S_PHONE CHAR(15),\n" + 
					"S_ACCTBAL DECIMAL(12,2),\n" + 
					"S_COMMENT VARCHAR(101)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS PARTSUPP(\n" + 
					"PS_PARTKEY BIGINT NOT NULL,\n" + 
					"PS_SUPPKEY BIGINT NOT NULL,\n" + 
					"PS_AVAILQTY INTEGER,\n" + 
					"PS_SUPPLYCOST DECIMAL(12,2),\n" + 
					"PS_COMMENT VARCHAR(199),\n" + 
					"CONSTRAINT PK PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS CUSTOMER(\n" + 
					"C_CUSTKEY BIGINT NOT NULL PRIMARY KEY,\n" + 
					"C_NAME VARCHAR(25),\n" + 
					"C_ADDRESS VARCHAR(40),\n" + 
					"C_NATIONKEY BIGINT,\n" + 
					"C_PHONE CHAR(15),\n" + 
					"C_ACCTBAL DECIMAL(12,2),\n" + 
					"C_MKTSEGMENT CHAR(10),\n" + 
					"C_COMMENT VARCHAR(117)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS ORDERS(\n" + 
					"O_ORDERKEY BIGINT NOT NULL PRIMARY KEY,\n" + 
					"O_CUSTKEY BIGINT,\n" + 
					"O_ORDERSTATUS CHAR(1),\n" + 
					"O_TOTALPRICE DECIMAL(12,2),\n" + 
					"O_ORDERDATE DATE,\n" + 
					"O_ORDERPRIORITY CHAR(15),\n" + 
					"O_CLERK CHAR(15),\n" + 
					"O_SHIPPRIORITY INTEGER,\n" + 
					"O_COMMENT VARCHAR(79)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS LINEITEM(\n" + 
					"L_ORDERKEY BIGINT NOT NULL,\n" + 
					"L_PARTKEY BIGINT,\n" + 
					"L_SUPPKEY BIGINT,\n" + 
					"L_LINENUMBER INTEGER NOT NULL,\n" + 
					"L_QUANTITY DECIMAL(12,2),\n" + 
					"L_EXTENDEDPRICE DECIMAL(12,2),\n" + 
					"L_DISCOUNT DECIMAL(12,2),\n" + 
					"L_TAX DECIMAL(12,2),\n" + 
					"L_RETURNFLAG CHAR(1),\n" + 
					"L_LINESTATUS CHAR(1),\n" + 
					"L_SHIPDATE DATE,\n" + 
					"L_COMMITDATE DATE,\n" + 
					"L_RECEIPTDATE DATE,\n" + 
					"L_SHIPINSTRUCT CHAR(25),\n" + 
					"L_SHIPMODE CHAR(10),\n" + 
					"L_COMMENT VARCHAR(44),\n" + 
					"CONSTRAINT PK PRIMARY KEY (L_ORDERKEY,L_LINENUMBER)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS NATION(\n" + 
					"N_NATIONKEY BIGINT NOT NULL PRIMARY KEY,\n" + 
					"N_NAME CHAR(25),\n" + 
					"N_REGIONKEY BIGINT,\n" + 
					"N_COMMENT VARCHAR(152)\n" + 
					")");
			statement.executeUpdate("CREATE TABLE REGION(\n" + 
					"R_REGIONKEY BIGINT NOT NULL PRIMARY KEY,\n" + 
					"R_NAME CHAR(25),\n" + 
					"R_COMMENT VARCHAR(152)\n" + 
					")");
			connection.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
