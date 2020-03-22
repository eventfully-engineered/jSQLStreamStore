package jsqlstreamstore;

import com.fasterxml.uuid.Generators;
import jsqlstreamstore.infrastructure.Ensure;
import jsqlstreamstore.store.ConnectionFactory;
import jsqlstreamstore.streams.ExpectedVersion;
import jsqlstreamstore.streams.NewStreamMessage;
import jsqlstreamstore.streams.ReadAllPage;
import jsqlstreamstore.streams.ReadDirection;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteStreamStoreTest {

//    final DriverManagerDataSource dataSource = new DriverManagerDataSource();
//    dataSource.setDriverClassName(env.getProperty("driverClassName"));
//    dataSource.setUrl(env.getProperty("url"));
//    dataSource.setUsername(env.getProperty("user"));
//    dataSource.setPassword(env.getProperty("password"));
//    return dataSource;

//    driverClassName=org.sqlite.JDBC
//        url=jdbc:sqlite:memory:myDb?cache=shared
//        username=sa
//    password=sa
//    hibernate.dialect=com.baeldung.dialect.SQLiteDialect
//    hibernate.hbm2ddl.auto=create-drop
//    hibernate.show_sql=true


    private Connection dbConnection;
    private SqliteStreamStore store;


    static class MemoizingConnectionFactory implements ConnectionFactory {
        volatile boolean initialized;
        String url;
        Connection value;

        MemoizingConnectionFactory(String url) {
            this.url = Ensure.notNull(url);
        }

        @Override
        public Connection openConnection() throws SQLException {
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        value = DriverManager.getConnection(url);
                        initialized = true;
                        return value;
                    }
                }
            }
            return value;
        }

    }

    static class MemoizingDataSourceConnectionFactory implements ConnectionFactory {
        volatile boolean initialized;
        DataSource ds;
        Connection value;

        MemoizingDataSourceConnectionFactory(DataSource ds) {
            this.ds = Ensure.notNull(ds);
        }

        @Override
        public Connection openConnection() throws SQLException {
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        value = ds.getConnection();
                        initialized = true;
                        return value;
                    }
                }
            }
            return value;
        }

    }


    @BeforeEach
    void setUp() throws Exception {
        // dbConnection = DriverManager.getConnection("jdbc:sqlite::memory:");
        String url = "jdbc:sqlite::memory:";
        // String url  = "jdbc:sqlite:streamstore.db";

//        SQLiteDataSource delegate = new SQLiteDataSource();
//        delegate.setUrl(url);
//        DataSource ds = new SingleConnectionDataSource(delegate);

        DataSource ds = new SingleConnectionDataSource(url);

        ConnectionFactory connectionFactory = () -> ds.getConnection();



//        ConnectionFactory connectionFactory = () -> DriverManager.getConnection(url);
//        Supplier<Connection> c = Suppliers.memoize(() -> {
//            try {
//                return DriverManager.getConnection(url);
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        });

        // SqliteStreamStoreSettings settings = new SqliteStreamStoreSettings.Builder(new MemoizingConnectionFactory(url)).build();
        SqliteStreamStoreSettings settings = new SqliteStreamStoreSettings.Builder(connectionFactory).build();
        // SqliteStreamStoreSettings settings = new SqliteStreamStoreSettings.Builder(new MemoizingDataSourceConnectionFactory(ds)).build();

        store = new SqliteStreamStore(settings);

        Flyway flyway = Flyway.configure()
            .dataSource(ds)
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
    }

    @Test
    void readAllForwardTest() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}"
        );

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[]{newMessage});

        ReadAllPage all = store.readAllForwards(0, 10, true);

        assertTrue(all.isEnd());
        assertEquals(ReadDirection.FORWARD, all.getReadDirection());
        assertEquals(1, all.getMessages().length);
        assertEquals(newMessage.getMessageId(), all.getMessages()[0].getMessageId());
    }
}
