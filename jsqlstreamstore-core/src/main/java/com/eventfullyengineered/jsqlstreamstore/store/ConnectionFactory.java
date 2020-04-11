package com.eventfullyengineered.jsqlstreamstore.store;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Supplies {@link Connection} instances.
 */
@FunctionalInterface
public interface ConnectionFactory {

    /**
     * @return a Connection
     * @throws SQLException if anything goes wrong
     */
    Connection openConnection() throws SQLException;
}
