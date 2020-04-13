package com.eventfullyengineered.jsqlstreamstore;

import com.eventfullyengineered.jsqlstreamstore.sqlite.SqliteStreamStore;
import com.eventfullyengineered.jsqlstreamstore.sqlite.SqliteStreamStoreSettings;
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
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.UUID;

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
        Assertions.assertEquals(ReadDirection.FORWARD, all.getReadDirection());
        assertEquals(1, all.getMessages().length);
        assertEquals(newMessage.getMessageId(), all.getMessages()[0].getMessageId());
    }

    @Test
    void readStreamForwards() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage});

        ReadStreamPage page = store.readStreamForwards("test", 0, 10, false);

        assertTrue(page.isEnd());
        assertEquals(ReadDirection.FORWARD, page.getReadDirection());
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessage.getMessageId(), page.getMessages()[0].getMessageId());
    }

    @Test
    void readStreamForwardsEqualToCount() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage});

        ReadStreamPage page = store.readStreamForwards("test", 0, 1, false);

        assertTrue(page.isEnd());
        assertEquals(ReadDirection.FORWARD, page.getReadDirection());
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessage.getMessageId(), page.getMessages()[0].getMessageId());
    }

    @Test
    void readAllForwardNext() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        NewStreamMessage newMessageToo = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Shawn\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage, newMessageToo});

        ReadAllPage page = store.readAllForwards(0, 1, false);
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessage.getMessageId(), page.getMessages().clone()[0].getMessageId());

        ReadAllPage page2 = page.readNext();
        assertEquals(1, page2.getMessages().length);
        assertEquals(newMessageToo.getMessageId(), page2.getMessages().clone()[0].getMessageId());
    }

    // TODO: add a test for max count equal to Integer.MAX_VALUE
    // TODO: add tests for unique constraint exception
    // TODO: add tests for SqlException none unique constraint exception
    // TODO: add test for read stream next page

    @Test
    void readAllBackwardsTest() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage});

        ReadAllPage all = store.readAllBackwards(Position.END, 10, false);

        assertTrue(all.isEnd());
        assertEquals(ReadDirection.BACKWARD, all.getReadDirection());
        assertEquals(1, all.getMessages().length);
        assertEquals(newMessage.getMessageId(), all.getMessages()[0].getMessageId());
    }


    @Test
    void readStreamBackwards() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        NewStreamMessage newMessageToo = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Shawn\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage, newMessageToo});

        ReadStreamPage page = store.readStreamBackwards("test", StreamVersion.END, 1, false);

        assertFalse(page.isEnd());
        assertEquals(ReadDirection.BACKWARD, page.getReadDirection());
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessageToo.getMessageId(), page.getMessages()[0].getMessageId());
    }

    @Test
    void appendStreamExpectedVersionNoStream() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\",\"type\":\"someType\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage});
        ReadStreamPage page = store.readStreamForwards("test", 0, 1, true);
        ReadStreamPage pageBackwards = store.readStreamBackwards("test", StreamVersion.END, 1, true);
        ReadAllPage allPage = store.readAllForwards(0, 10, true);

        assertNotNull(page);
        assertNotNull(allPage);
        assertEquals(1, allPage.getMessages().length);
        Assertions.assertEquals(PageReadStatus.SUCCESS, page.getStatus());
        assertEquals(1, page.getMessages().length);
        assertEquals(1, pageBackwards.getMessages().length);
    }

    @Test
    void appendStreamExpectedVersionAny() throws SQLException {
        store.appendToStream("test", ExpectedVersion.ANY, createNewStreamMessages(1, 2));
        ReadStreamPage page = store.readStreamForwards("test", 0, 2, true);

        assertNotNull(page);
        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
        assertEquals(2, page.getMessages().length);
    }

    // TODO: When_append_stream_second_time_with_no_stream_expected_and_different_message_then_should_throw

    @Test
    void When_append_stream_second_time_with_no_stream_expected_and_same_messages_then_should_then_should_be_idempotent() throws SQLException {
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));

        ReadStreamPage page = store.readStreamForwards("test", 0, 10, true);

        assertNotNull(page);
        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
        assertEquals(2, page.getMessages().length);
    }

//    @Test
//    public void appendStreamIdempotent() throws SQLException {
//        NewStreamMessage newMessage = new NewStreamMessage(
//            Generators.timeBasedGenerator().generate(),
//            "someType",
//            "{\"name\":\"Sean\"}");
//
//        AppendResult result = store.appendToStream("test", ExpectedVersion.ANY, new NewStreamMessage[] {newMessage, newMessage});
//
//        ReadStreamPage page = store.readStreamForwards("test", 0, 10, true);
//
//        assertNotNull(page);
//        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
//        assertEquals(1, page.getMessages().length);
//    }

//    @Test
//    public void appendStreamIdempotentSeparateInserts() throws SQLException {
//        NewStreamMessage newMessage = new NewStreamMessage(
//            Generators.timeBasedGenerator().generate(),
//            "someType",
//            "{\"name\":\"Sean\"}");
//
//        AppendResult result = store.appendToStream("test", ExpectedVersion.ANY, new NewStreamMessage[] {newMessage});
//        AppendResult resultTwo = store.appendToStream("test", ExpectedVersion.ANY, new NewStreamMessage[] {newMessage});
//        ReadStreamPage page = store.readStreamForwards("test", 0, 10, true);
//
//        assertNotNull(page);
//        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
//        assertEquals(1, page.getMessages().length);
//    }

//    @Test
//    public void appendStreamIdempotentFail() throws SQLException {
//
//        UUID id = Generators.timeBasedGenerator().generate();
//        NewStreamMessage newMessage = new NewStreamMessage(
//            id,
//            "someType",
//            "{\"name\":\"Sean\"}");
//
//        NewStreamMessage newMessageTwo = new NewStreamMessage(
//            id,
//            "someType",
//            "{\"name\":\"Dan\"}");
//
//        AppendResult result = store.appendToStream("test", ExpectedVersion.ANY, new NewStreamMessage[] {newMessage});
//
//        try {
//            AppendResult resultTwo = store.appendToStream("test", ExpectedVersion.ANY, new NewStreamMessage[]{newMessageTwo});
//            fail("should throw");
//        } catch(Exception ex) {
//
//        }
//
//        ReadStreamPage page = store.readStreamForwards("test", 0, 10, true);
//
//        assertNotNull(page);
//        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
//        assertEquals(1, page.getMessages().length);
//    }


    // TODO: add tests for appendStreamExpectedVersion
    // TODO: we are also missing the sql script for appendStreamExpectedVersion
    // TODO: we also have AppendStreamAnyVersion and AppendStreamExpectedVersionAny which feel like the same thing to me

    @Test
    void deleteStream() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage});

        store.deleteStream("test", ExpectedVersion.ANY);

        ReadStreamPage page = store.readStreamForwards("test", 0, 1, true);
        assertEquals(PageReadStatus.STREAM_NOT_FOUND, page.getStatus());
    }

    @Test
    void deleteMessage() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        NewStreamMessage newMessageToo = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Shawn\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage, newMessageToo});

        store.deleteMessage("test", newMessage.getMessageId());

        ReadStreamPage page = store.readStreamForwards("test", 0, 10, false);
        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessageToo.getMessageId(), page.getMessages().clone()[0].getMessageId());
    }

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
