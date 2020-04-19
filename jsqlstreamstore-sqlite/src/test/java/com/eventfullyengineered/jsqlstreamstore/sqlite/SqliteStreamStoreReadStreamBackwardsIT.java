package com.eventfullyengineered.jsqlstreamstore.sqlite;

import com.eventfullyengineered.jsqlstreamstore.streams.ExpectedVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.Position;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadAllPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadDirection;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadStreamPage;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamVersion;
import com.fasterxml.uuid.Generators;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteStreamStoreReadStreamBackwardsIT extends BaseIT {

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
    void readStreamBackwardsNextPage() throws SQLException {
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

}
