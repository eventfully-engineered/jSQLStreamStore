package com.eventfullyengineered.jsqlstreamstore.sqlite;

import com.eventfullyengineered.jsqlstreamstore.streams.ExpectedVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadDirection;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadStreamPage;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamVersion;
import com.fasterxml.uuid.Generators;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteStreamStoreReadStreamForwardIT extends BaseIT {

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
    void readStreamForwardsNextPage() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        NewStreamMessage newMessageToo = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Shawn\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage, newMessageToo});

        ReadStreamPage page = store.readStreamForwards("test", StreamVersion.START, 1, false);

        assertFalse(page.isEnd());
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessage.getMessageId(), page.getMessages().clone()[0].getMessageId());

        ReadStreamPage page2 = page.readNext();
        assertTrue(page2.isEnd());
        assertEquals(1, page2.getMessages().length);
        assertEquals(newMessageToo.getMessageId(), page2.getMessages().clone()[0].getMessageId());
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

}
