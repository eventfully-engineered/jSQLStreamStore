package jsqlstreamstore;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

interface ILocalInstance {
    Connection createConnection() throws SQLException;
    // SqlConnectionStringBuilder CreateConnectionStringBuilder();
    CompletableFuture createDatabase() throws SQLException;
}
