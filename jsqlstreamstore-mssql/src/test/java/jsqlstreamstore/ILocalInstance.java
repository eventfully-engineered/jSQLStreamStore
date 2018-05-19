package jsqlstreamstore;

import java.sql.Connection;
import java.sql.SQLException;

interface ILocalInstance {
    Connection createConnection() throws SQLException;
//    SqlConnectionStringBuilder CreateConnectionStringBuilder();
}
