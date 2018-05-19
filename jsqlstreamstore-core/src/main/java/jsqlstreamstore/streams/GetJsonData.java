package jsqlstreamstore.streams;

import java.sql.SQLException;

/**
 *
 *
 */
@FunctionalInterface
public interface GetJsonData {

    /**
     *
     * @return
     * @throws SQLException
     */
    String get() throws SQLException;

}
