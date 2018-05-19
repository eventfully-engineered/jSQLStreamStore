package jsqlstreamstore.streams;

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
    ReadStreamPage get(int nextVersion) throws SQLException;
}
