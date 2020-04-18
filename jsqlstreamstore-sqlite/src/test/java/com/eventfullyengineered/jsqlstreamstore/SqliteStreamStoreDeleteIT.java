package com.eventfullyengineered.jsqlstreamstore;

import com.eventfullyengineered.jsqlstreamstore.streams.ExpectedVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.PageReadStatus;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadStreamPage;
import com.fasterxml.uuid.Generators;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqliteStreamStoreDeleteIT extends BaseIT {

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

}
