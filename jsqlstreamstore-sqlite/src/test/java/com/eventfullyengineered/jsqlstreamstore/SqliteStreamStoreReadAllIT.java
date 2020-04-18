package com.eventfullyengineered.jsqlstreamstore;

import com.eventfullyengineered.jsqlstreamstore.streams.ExpectedVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadAllPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadDirection;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamVersion;
import com.fasterxml.uuid.Generators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqliteStreamStoreReadAllIT extends BaseIT {

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
    void readAllForwardNextPage() throws SQLException {
        NewStreamMessage newMessage = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Sean\"}");

        NewStreamMessage newMessageToo = new NewStreamMessage(
            Generators.timeBasedGenerator().generate(),
            "someType",
            "{\"name\":\"Shawn\"}");

        store.appendToStream("test", ExpectedVersion.NO_STREAM, new NewStreamMessage[] {newMessage, newMessageToo});

        ReadAllPage page = store.readAllForwards(StreamVersion.START, 1, false);
        assertFalse(page.isEnd());
        assertEquals(1, page.getMessages().length);
        assertEquals(newMessage.getMessageId(), page.getMessages().clone()[0].getMessageId());

        ReadAllPage page2 = page.readNext();
        assertTrue(page2.isEnd());
        assertEquals(1, page2.getMessages().length);
        assertEquals(newMessageToo.getMessageId(), page2.getMessages().clone()[0].getMessageId());
    }

}
