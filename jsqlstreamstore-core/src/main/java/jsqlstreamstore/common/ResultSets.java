package jsqlstreamstore.common;


import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

public final class ResultSets {

    private ResultSets() {
        // statics only
    }

    public static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        // TODO: what to do with null?
        return timestamp.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
    }

}
