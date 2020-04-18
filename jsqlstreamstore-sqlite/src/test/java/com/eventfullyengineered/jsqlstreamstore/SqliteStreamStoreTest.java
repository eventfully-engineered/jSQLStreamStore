package com.eventfullyengineered.jsqlstreamstore;

import com.eventfullyengineered.jsqlstreamstore.sqlite.SqliteStreamStore;
import com.eventfullyengineered.jsqlstreamstore.sqlite.SqliteStreamStoreSettings;
import com.eventfullyengineered.jsqlstreamstore.streams.AppendResult;
import com.eventfullyengineered.jsqlstreamstore.streams.ExpectedVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.PageReadStatus;
import com.eventfullyengineered.jsqlstreamstore.streams.Position;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadAllPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadDirection;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadStreamPage;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamVersion;
import com.fasterxml.uuid.Generators;
import com.eventfullyengineered.jsqlstreamstore.store.ConnectionFactory;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

//    @Test
//    void shouldThrowWhenAppendingNullMessages() {
//        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> store.appendToStream("test", ExpectedVersion.NO_STREAM, (NewStreamMessage) null));
//        assertEquals("messages cannot be null or empty", ex.getMessage());
//    }

    // 80 - 206 ms
//    @Test
//    void appendPerf() throws SQLException {
//
//        List<NewStreamMessage> messages = new ArrayList<>(1000);
//        for (int i = 0; i < 1000; i++) {
//            UUID id = Generators.timeBasedGenerator().generate();
//            messages.add(new NewStreamMessage(id, "someType", "{\"name\":\"" + id + "\"}"));
//        }
//
//        NewStreamMessage[] arr =  messages.toArray(new NewStreamMessage[0]);
//
//        Stopwatch sw = Stopwatch.createStarted();
//        AppendResult result = store.appendToStream("test", ExpectedVersion.NO_STREAM, arr);
//        sw.stop();
//
//        System.out.println(result);
//        System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
//    }


    // TODO: add tests for appendStreamExpectedVersion
    // TODO: we are also missing the sql script for appendStreamExpectedVersion
    // TODO: we also have AppendStreamAnyVersion and AppendStreamExpectedVersionAny which feel like the same thing to me

    // TODO: add test for expired messages



    public static NewStreamMessage[] createNewStreamMessages(int... messageNumbers) {
        return createNewStreamMessages("{\"message\": \"hello\"}", messageNumbers);
    }

    public static NewStreamMessage[] createNewStreamMessages(String jsonData, int[] messageNumbers) {
        NewStreamMessage[] newMessages = new NewStreamMessage[messageNumbers.length];
        for (int i = 0; i < messageNumbers.length; i++) {
            UUID id = UUID.fromString(StringUtils.leftPad("00000000-0000-0000-0000-" + messageNumbers[i], 12, "0"));
            newMessages[i] = new NewStreamMessage(id, "type", jsonData, "{\"foo\": \"baz\"}");
        }
        return newMessages;
    }

}
