package jsqlstreamstore.infrastructure;

import java.util.UUID;

public class Utils {

    private Utils() {
        // private static only
    }

    public static boolean isUUID(String string) {
        try {
            UUID.fromString(string);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
