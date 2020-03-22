package jsqlstreamstore;

import com.fasterxml.uuid.Generators;
import jsqlstreamstore.store.ConnectionFactory;
import jsqlstreamstore.streams.ExpectedVersion;
import jsqlstreamstore.streams.NewStreamMessage;
import jsqlstreamstore.streams.ReadAllPage;
import jsqlstreamstore.streams.ReadDirection;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteStreamStoreTest {

    private SqliteStreamStore store;

    @BeforeEach
    void setUp() throws Exception {
        String url = "jdbc:sqlite::memory:";
        DataSource ds = new SingleConnectionDataSource(url);
        ConnectionFactory connectionFactory = ds::getConnection;
        SqliteStreamStoreSettings settings = new SqliteStreamStoreSettings.Builder(connectionFactory).build();
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
