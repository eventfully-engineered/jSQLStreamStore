package jsqlstreamstore;

import com.fasterxml.uuid.Generators;
import org.flywaydb.core.Flyway;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import static ru.yandex.qatools.embed.postgresql.EmbeddedPostgres.cachedRuntimeConfig;


public class MsSqlStreamStoreTest {

    private String connectionString;
    private String _schema;
    private String _databaseName;
    private ILocalInstance _localInstance;

    @Before
    public void setUp() throws Exception {
        // TODO: move this to fixture
        _schema = "public"; //schema;
        _localInstance = new LocalInstance();

        String uniqueName = UUID.randomUUID().toString().replaceAll("-", "");
        _databaseName = "StreamStoreTests-{uniqueName}";

        connectionString = CreateConnectionString();


//        postgres = new EmbeddedPostgres();
//        String url = postgres.start(cachedRuntimeConfig(Paths.get(System.getProperty("java.io.tmpdir"), "pgembed")));
//
//        PostgresStreamStoreSettings settings = new PostgresStreamStoreSettings.Builder(url).build();
//
//        store = new PostgresStreamStore(settings);

        Flyway flyway = new Flyway();
        flyway.setDataSource(connectionString, EmbeddedPostgres.DEFAULT_USER, EmbeddedPostgres.DEFAULT_PASSWORD);
        flyway.setLocations("classpath:db/migrations");
        flyway.migrate();
    }

    @After
    public void tearDown() throws Exception {
//        if (postgres != null && postgres.getProcess().isPresent()) {
//            postgres.stop();
//        }
        // postgres.getProcess().ifPresent(PostgresProcess::stop);
    }



    private void createDatabase() throws SQLException {

        try (Connection connection = _localInstance.createConnection()) {
            String temp = System.getProperty("java.io.tmpdir");
            String createDatabase = "CREATE DATABASE [{_databaseName}] on (name='{_databaseName}', filename='{tempPath}\\{_databaseName}.mdf')";

            try (PreparedStatement ps = connection.prepareStatement(createDatabase)) {
                ps.execute();
            }
        }

    }

    private String CreateConnectionString()
    {
//        var connectionStringBuilder = _localInstance.CreateConnectionStringBuilder();
//        connectionStringBuilder.MultipleActiveResultSets = true;
//        connectionStringBuilder.IntegratedSecurity = true;
//        connectionStringBuilder.InitialCatalog = _databaseName;
//
//        return connectionStringBuilder.ToString();

        return null;
    }

}
