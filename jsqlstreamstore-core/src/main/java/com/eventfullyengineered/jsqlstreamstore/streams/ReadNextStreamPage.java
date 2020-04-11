package com.eventfullyengineered.jsqlstreamstore.streams;

import java.sql.SQLException;

/**
 *
 *
 */
@FunctionalInterface
public interface ReadNextStreamPage {

    /**
     *
     * @param nextVersion
     * @return
     */
    ReadStreamPage get(long nextVersion) throws SQLException;
}
