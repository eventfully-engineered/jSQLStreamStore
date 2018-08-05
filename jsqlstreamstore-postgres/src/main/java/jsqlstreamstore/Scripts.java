package jsqlstreamstore;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author seancarroll
 *
 */
public class Scripts {
    private final String schema;
    private final ConcurrentMap<String, String> sqlScripts = new ConcurrentHashMap<>();

    public Scripts(String schema) {
        this.schema = schema;
    }

    public String getAppendStreamExpectedVersionAny() {
        return getScript("AppendStreamExpectedVersionAny");
    }

    public String getAppendStreamExpectedVersion() {
        return getScript("AppendStreamExpectedVersion");
    }

    public String getAppendStreamExpectedVersionNoStream() {
        return getScript("AppendStreamExpectedVersionNoStream");
    }

    public String getStreamMessageCount() {
        return getScript("GetStreamMessageCount");
    }

    public String getStreamVersionOfMessageId() {
        return getScript("GetStreamVersionOfMessageId");
    }

    public String readHeadPosition() {
        return getScript("ReadHeadPosition");
    }

    public String readAllForward() {
        return getScript("ReadAllForward");
    }

    public String readAllForwardWithData() {
        return getScript("ReadAllForwardWithData");
    }

    public String readStreamForward() {
        return getScript("ReadStreamForward");
    }

    public String readStreamForwardWithData() {
        return getScript("ReadStreamForwardWithData");
    }

    public String readAllBackward() {
        return getScript("ReadAllBackward");
    }

    public String readAllBackwardWithData() {
        return getScript("ReadAllBackwardWithData");
    }

    public String readStreamBackwards() {
        return getScript("ReadStreamBackwards");
    }

    public String readStreamBackwardsWithData() {
        return getScript("ReadStreamBackwardsWithData");
    }

    public String readMessageData() {
        return getScript("ReadMessageData");
    }

    public String deleteStreamExpectedVersion() {
        return getScript("DeleteStreamVersion");
    }

    public String deleteStreamMessage() {
        return getScript("DeleteStreamMessage");
    }

    public String deleteStreamAnyVersion() {
        return getScript("DeleteStreamAnyVersion");
    }

    private String getScript(String name) {
        String script = sqlScripts.get(name);
        if (script == null) {
            InputStream resourceStream = getClass().getClassLoader().getResourceAsStream("db/scripts/" + name + ".sql");
            if (resourceStream == null) {
                throw new RuntimeException("Resource script " + name + " not found");
            }
            try {
                script = readInputStreamAsString(resourceStream).replaceAll("public.", schema + ".");
                sqlScripts.put(name, script);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return script;
    }

    private static String readInputStreamAsString(InputStream in) throws IOException {
        BufferedInputStream bis = new BufferedInputStream(in);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int result = bis.read();
        while (result != -1) {
            byte b = (byte) result;
            buf.write(b);
            result = bis.read();
        }
        return buf.toString();
    }
}
