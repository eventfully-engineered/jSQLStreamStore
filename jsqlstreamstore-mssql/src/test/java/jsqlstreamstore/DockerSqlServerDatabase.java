package jsqlstreamstore;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class DockerSqlServerDatabase implements ILocalInstance {

    private String _databaseName;
//    private DockerContainer _sqlServerContainer;
    private String _password;
    private final String Image = "microsoft/mssql-server-linux";
    private final String Tag = "2017-CU5";
    private final int Port = 1433;

    @Override
    public Connection createConnection() throws SQLException {
        return null;
    }

    @Override
    public CompletableFuture createDatabase() throws SQLException {
        return null;
    }
}
