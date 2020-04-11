package com.eventfullyengineered.jsqlstreamstore.infrastructure;

import com.google.common.base.Strings;

public final class SystemNames {

    private SystemNames() {
        // static only
    }

    /* Shouldn't need these
     *     public static class SystemHeaders
            {
                public const string ExpectedVersion = "ES-ExpectedVersion";
                public const string RequireMaster = "ES-RequireMaster";
                public const string ResolveLinkTos = "ES-ResolveLinkTos";
                public const string LongPoll = "ES-LongPoll";
                public const string TrustedAuth = "ES-TrustedAuth";
                public const string ProjectionPosition = "ES-Position";
                public const string HardDelete = "ES-HardDelete";
                public const string EventId = "ES-EventId";
                public const string EventType = "ES-EventType";
                public const string CurrentVersion = "ES-CurrentVersion";
            }
     */

    public final static class SystemStreams {
        public static final String ALL_STREAM = "$all";
        public static final String STREAMS_STREAM = "$jsqlstreamstore.streams";
        public static final String SETTINGS_STREAM = "$settings";
        public static final String STATS_STREAM_PREFIX = "$stats";

        private SystemStreams() {
            // static constants only
        }

        public static boolean isSystemStream(String streamId) {
            return Strings.isNullOrEmpty(streamId)
                    && streamId.length() != 0
                    && streamId.charAt(0) == '$';
        }

        public static String metastreamOf(String streamId) {
            return "$$" + streamId;
        }

        public static boolean isMetastream(String streamId) {
            // return streamId.length() >= 2 && streamId.charAt(0) == '$' && streamId.charAt(1) == '$';
            return streamId.startsWith("$$");
        }

        public static String originalStreamOf(String metastreamId) {
            return metastreamId.substring(2);
        }
    }

    /**
     * Constants for information in stream metadata
     *
     */
    public final class SystemMetadata {

        /**
         * The definition of the max age value assigned to stream metadata
         * Setting this allows all events older than the limit to be deleted
         */
        public static final String MAX_AGE = "$maxAge";

        /**
         * The definition of the max count value assigned to the stream metadata
         * Setting this allows all events with a sequence less than current -maxcount to be deleted
         */
        public static final String MAX_COUNT = "$maxCount";

        private SystemMetadata() {
            // static constants only
        }
    }

    public static class SystemEventTypes {
        //private static readonly char[] _linkToSeparator = new []{'@'};
        public static final String STREAM_DELETED = "$streamDeleted";
        public static final String STATS_COLLECTION = "$statsCollected";
        public static final String LINK_TO = "$>";
        public static final String STREAM_REFERENCE = "$@";
        public static final String STREAM_METADATA = "$metadata";
        public static final String SETTINGS = "$settings";

        private SystemEventTypes() {
            // static constants only
        }
    }

    public static class SystemUsers {
        public static final String ADMIN = "admin";
        public static final String OPERATIONS = "ops";
        public static final String DEFAULT_ADMIN_PASSWORD = "changeit";

        private SystemUsers() {
            // static constants only
        }
    }

    public static class SystemRoles {
        public static final String ADMINS = "$admins";
        public static final String OPERATIONS = "$ops";
        public static final String ALL = "$all";

        private SystemRoles() {
            // static constants only
        }
    }

    /**
     * System supported consumer strategies for use with persistent jsqlstreamstore.subscriptions.
     *
     */
    public static class SystemConsumerStrategies {

        /**
         * Distributes events to a single client until it is full. Then round robin to the next client.
         */
        public final static String DISPATCH_TO_SINGLE = "DispatchToSingle";

        /**
         * Distribute events to each client in a round robin fashion.
         */
        public static final String ROUND_ROBIN = "RoundRobin";

        /**
         * Distribute events of the same streamId to the same client until it disconnects on a best efforts basis.
         * Designed to be used with indexes such as the category projection.
         */
        public static final String PINNED = "Pinned";

        private SystemConsumerStrategies() {
            // static constants only
        }
    }

}
