package jsqlstreamstore.infrastructure.serialization;

/**
 *
 *
 */
public interface JsonSerializerStrategy {

    /**
     *
     * @param o
     * @return
     */
    String toJson(Object o);

    /**
     *
     * @param s
     * @param type
     * @return
     */
    <T> T fromJson(String s, Class<T> type);

}
