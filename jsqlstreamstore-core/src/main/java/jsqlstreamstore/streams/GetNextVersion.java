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
    int get(List<StreamMessage> messages, int lastVersion);
}
