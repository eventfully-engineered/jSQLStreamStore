package jsqlstreamstore;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MsSqlStreamStoreTest {
    private final MSSQLServerContainer mssqlserver = new MSSQLServerContainer();
//    private SqlS store;

    @BeforeEach
    void before() {
        mssqlserver.start();

//        PostgresStreamStoreSettings settings = new PostgresStreamStoreSettings.Builder(url).build();
//        store = new PostgresStreamStore(settings);

        Flyway flyway = Flyway.configure()
            .dataSource(mssqlserver.getJdbcUrl(), mssqlserver.getUsername(), mssqlserver.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
    }

    @AfterEach
    void after() {
        mssqlserver.stop();
    }



}
