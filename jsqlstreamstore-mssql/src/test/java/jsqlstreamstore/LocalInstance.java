package jsqlstreamstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class LocalInstance implements ILocalInstance {
    private String connectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=master;Integrated Security=SSPI;";

    public Connection createConnection() throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

//    public SqlConnectionStringBuilder CreateConnectionStringBuilder() {
//        return new SqlConnectionStringBuilder(connectionString);
//    }
}
