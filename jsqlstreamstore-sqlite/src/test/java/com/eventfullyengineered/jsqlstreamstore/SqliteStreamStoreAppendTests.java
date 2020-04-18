package com.eventfullyengineered.jsqlstreamstore;

import com.eventfullyengineered.jsqlstreamstore.streams.ExpectedVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.NewStreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.PageReadStatus;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadAllPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadStreamPage;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamVersion;
import com.eventfullyengineered.jsqlstreamstore.streams.WrongExpectedVersion;
import com.fasterxml.uuid.Generators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteStreamStoreAppendTests extends BaseIT {

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

    // TODO: add a test for max count equal to Integer.MAX_VALUE

    @Test
    void higherExpectedVersionShouldFailWithWrongExpectedVersion() throws SQLException {
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));

        WrongExpectedVersion wrongExpectedVersion =
            assertThrows(WrongExpectedVersion.class, () -> store.appendToStream("test", 3, createNewStreamMessages(3, 4)));

        assertTrue(wrongExpectedVersion.getMessage().contains("WrongExpectedVersion: 3"));
        assertTrue(wrongExpectedVersion.getMessage().contains("Stream: test"));
        assertTrue(wrongExpectedVersion.getMessage().contains("Stream version: 1"));
    }

    @Test
    void nonIdempotentAppendShouldThrowWithWrongExpectedVersion() throws SQLException {
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));

        WrongExpectedVersion wrongExpectedVersion =
            assertThrows(WrongExpectedVersion.class, () -> store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(2, 3)));

        assertTrue(wrongExpectedVersion.getMessage().contains("WrongExpectedVersion: -1"));
        assertTrue(wrongExpectedVersion.getMessage().contains("Stream: test"));
        assertTrue(wrongExpectedVersion.getMessage().contains("Stream version: 1"));
    }

    @Test
    void appendingSecondTimeWithMoreMessagesShouldThrowWrongExpectedVersion() throws SQLException {
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));

        WrongExpectedVersion wrongExpectedVersion =
            assertThrows(WrongExpectedVersion.class, () -> store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2, 3)));

        assertTrue(wrongExpectedVersion.getMessage().contains("WrongExpectedVersion: -1"));
        assertTrue(wrongExpectedVersion.getMessage().contains("Stream: test"));
        assertTrue(wrongExpectedVersion.getMessage().contains("Stream version: 1"));
    }

    @Test
    void appendingSecondTimeWithSameMessagesShouldBeIdempotent() throws SQLException {
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));
        store.appendToStream("test", ExpectedVersion.NO_STREAM, createNewStreamMessages(1, 2));

        ReadStreamPage page = store.readStreamForwards("test", 0, 10, true);

        assertNotNull(page);
        assertEquals(PageReadStatus.SUCCESS, page.getStatus());
        assertEquals(2, page.getMessages().length);
    }

}
