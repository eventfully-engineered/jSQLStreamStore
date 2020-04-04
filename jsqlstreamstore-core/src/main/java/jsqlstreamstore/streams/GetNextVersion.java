package jsqlstreamstore.streams;

import java.util.List;

/**
 *
 *
 */
@FunctionalInterface
public interface GetNextVersion {

    /**
     *
     * @param messages
     * @param lastVersion
     * @return
     */
    long get(List<StreamMessage> messages, long lastVersion);
}
