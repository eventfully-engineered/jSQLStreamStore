package com.eventfullyengineered.jsqlstreamstore.sqlite;

import com.eventfullyengineered.jsqlstreamstore.sqlite.sqlite.SqliteStreamStore;
import com.eventfullyengineered.jsqlstreamstore.sqlite.sqlite.SqliteStreamStoreSettings;
import com.eventfullyengineered.jsqlstreamstore.store.ConnectionFactory;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;

import javax.sql.DataSource;
import java.util.UUID;

class BaseIT {

    protected SqliteStreamStore store;

    @BeforeEach
    void setUp() throws Exception {
        String url = "jdbc:sqlite::memory:";
        DataSource ds = new SingleConnectionDataSource(url);
        ConnectionFactory connectionFactory = ds::getConnection;
        SqliteStreamStoreSettings settings = new SqliteStreamStoreSettings.Builder(connectionFactory).build();
        store = new SqliteStreamStore(settings);

        Flyway flyway = Flyway.configure()
            .dataSource(ds)
            .load();
        flyway.migrate();
    }


    protected static NewStreamMessage[] createNewStreamMessages(int... messageNumbers) {
        return createNewStreamMessages("{\"message\": \"hello\"}", messageNumbers);
    }

    protected static NewStreamMessage[] createNewStreamMessages(String data, int[] messageNumbers) {
        NewStreamMessage[] newMessages = new NewStreamMessage[messageNumbers.length];
        for (int i = 0; i < messageNumbers.length; i++) {
            UUID id = UUID.fromString(StringUtils.leftPad("00000000-0000-0000-0000-" + messageNumbers[i], 12, "0"));
            newMessages[i] = new NewStreamMessage(id, "type", data, "{\"foo\": \"baz\"}");
        }
        return newMessages;
    }

}
