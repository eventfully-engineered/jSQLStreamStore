package jsqlstreamstore;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

class LocalInstance implements ILocalInstance {

    private final String connectionString = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=master;Integrated Security=SSPI;";

    private final String _databaseName;

    public LocalInstance(String databaseName) {
        _databaseName = databaseName;
    }

    public Connection createConnection() throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

    @Override
    public CompletableFuture createDatabase() throws SQLException {
        try (Connection connection = createConnection()) {

            String tempPath = System.getProperty("java.io.tmpdir");
            String createDatabase = String.format("CREATE DATABASE [%s] on (name='%s' filename='%s\\%s.mdf')",
                _databaseName, _databaseName, tempPath, _databaseName);

            try (PreparedStatement stmt = connection.prepareStatement(createDatabase)) {
                stmt.execute();
            }

//            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
//            var tempPath = Environment.GetEnvironmentVariable("Temp");
//            var createDatabase = $"CREATE DATABASE [{_databaseName}] on (name='{_databaseName}', "
//            + $"filename='{tempPath}\\{_databaseName}.mdf')";
//            using (var command = new SqlCommand(createDatabase, connection))
//            {
//                await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
//            }
        }
        return null;
    }

//    public SqlConnectionStringBuilder CreateConnectionStringBuilder() {
//        return new SqlConnectionStringBuilder(connectionString);
//    }
}
