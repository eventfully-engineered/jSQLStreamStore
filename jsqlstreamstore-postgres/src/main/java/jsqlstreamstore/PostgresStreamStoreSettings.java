package jsqlstreamstore;

import jsqlstreamstore.infrastructure.Ensure;
import jsqlstreamstore.infrastructure.serialization.JacksonSerializer;
import jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import jsqlstreamstore.store.ConnectionFactory;
import jsqlstreamstore.subscriptions.CreateStreamStoreNotifier;
import jsqlstreamstore.subscriptions.PollingStreamStoreNotifier;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Properties;

// TODO: compare Marten StoreOptions and AdvancedOptions
public class PostgresStreamStoreSettings {

    // TODO: change default schema to something other than default
    private static final String DEFAULT_SCHEMA = "public";
    private static final JsonSerializerStrategy DEFAULT_JSON_SERIALIZER_STRATEGY = JacksonSerializer.DEFAULT;
    private static final Duration DEFAULT_METADATA_MAX_AGE_CACHE_EXPIRE = Duration.ofMinutes(1);
    private static final int DEFAULT_METADATA_MAX_AGE_CACHE_MAX_SIZE = 10000;
    private static final CreateStreamStoreNotifier DEFAULT_CREATE_STREAM_STORE_NOTIFIER = store -> new PollingStreamStoreNotifier(store);

    private String schema = DEFAULT_SCHEMA;
    private ConnectionFactory connectionFactory;
    private JsonSerializerStrategy jsonSerializerStrategy = DEFAULT_JSON_SERIALIZER_STRATEGY;
    private CreateStreamStoreNotifier createStreamStoreNotifier = DEFAULT_CREATE_STREAM_STORE_NOTIFIER;
    private Duration metadataMaxAgeCacheExpire = DEFAULT_METADATA_MAX_AGE_CACHE_EXPIRE;
    private int metadataMaxAgeCacheMaxSize = DEFAULT_METADATA_MAX_AGE_CACHE_MAX_SIZE;

    public void setSchema(String schema) {
        Ensure.notNullOrEmpty(schema, "schema");
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = Ensure.notNull(connectionFactory);
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setJsonSerializerStrategy(JsonSerializerStrategy strategy) {
        this.jsonSerializerStrategy = Ensure.notNull(strategy);
    }

    public JsonSerializerStrategy getJsonSerializerStrategy() {
        return jsonSerializerStrategy;
    }

    public void setCreateStreamStoreNotifier(CreateStreamStoreNotifier createStreamStoreNotifier) {
        this.createStreamStoreNotifier = createStreamStoreNotifier;
    }

    public CreateStreamStoreNotifier getCreateStreamStoreNotifier() {
        return createStreamStoreNotifier;
    }

    public void setMetadataMaxAgeCacheExpire(Duration metadataMaxAgeCacheExpire) {
        this.metadataMaxAgeCacheExpire = metadataMaxAgeCacheExpire;
    }

    public Duration getMetadataMaxAgeCacheExpire() {
        return metadataMaxAgeCacheExpire;
    }

    public void setMetadataMaxAgeCacheMaxSize(int metadataMaxAgeCacheMaxSize) {
        this.metadataMaxAgeCacheMaxSize = metadataMaxAgeCacheMaxSize;
    }

    public int getMetadataMaxAgeCacheMaxSize() {
        return metadataMaxAgeCacheMaxSize;
    }

    public static class Builder {

        private ConnectionFactory connectionFactory;
        private String schema = DEFAULT_SCHEMA;
        private JsonSerializerStrategy jsonSerializerStrategy = DEFAULT_JSON_SERIALIZER_STRATEGY;
        private CreateStreamStoreNotifier createStreamStoreNotifier = DEFAULT_CREATE_STREAM_STORE_NOTIFIER;
        private Duration metadataMaxAgeCacheExpire = DEFAULT_METADATA_MAX_AGE_CACHE_EXPIRE;
        private int metadataMaxAgeCacheMaxSize = DEFAULT_METADATA_MAX_AGE_CACHE_MAX_SIZE;

        public Builder(DataSource dataSource) {
            this(dataSource::getConnection);
        }

        public Builder(final String url) {
            this(() -> DriverManager.getConnection(url));
        }

        public Builder(final String url, final Properties properties) {
            this(() -> DriverManager.getConnection(url, properties));
        }

        public Builder(final String url, final String username, final String password) {
            this(() -> DriverManager.getConnection(url, username, password));
        }

        public Builder(ConnectionFactory connectionFactory) {
            Ensure.notNull(connectionFactory);
            this.connectionFactory = connectionFactory;
        }

        public Builder withSchema(String schema) {
            Ensure.notNull(schema);
            this.schema = schema;
            return this;
        }

        public Builder withJsonSerializerStrategy(JsonSerializerStrategy jsonSerializerStrategy) {
            Ensure.notNull(jsonSerializerStrategy);
            this.jsonSerializerStrategy = jsonSerializerStrategy;
            return this;
        }

        public Builder withStreamStoreNotifier(CreateStreamStoreNotifier createStreamStoreNotifier) {
            Ensure.notNull(createStreamStoreNotifier);
            this.createStreamStoreNotifier = createStreamStoreNotifier;
            return this;
        }

        public Builder withMetadataMaxAgeCacheExpire(Duration metadataMaxAgeCacheExpire) {
            Ensure.notNull(metadataMaxAgeCacheExpire);
            this.metadataMaxAgeCacheExpire = metadataMaxAgeCacheExpire;
            return this;
        }

        public Builder withMetadataMaxAgeCacheMaxSize(int metadataMaxAgeCacheMaxSize) {
            this.metadataMaxAgeCacheMaxSize = metadataMaxAgeCacheMaxSize;
            return this;
        }

        public PostgresStreamStoreSettings build() {
            PostgresStreamStoreSettings settings = new PostgresStreamStoreSettings();
            settings.setConnectionFactory(connectionFactory);
            settings.setCreateStreamStoreNotifier(createStreamStoreNotifier);
            settings.setJsonSerializerStrategy(jsonSerializerStrategy);
            settings.setMetadataMaxAgeCacheExpire(metadataMaxAgeCacheExpire);
            settings.setMetadataMaxAgeCacheMaxSize(metadataMaxAgeCacheMaxSize);
            settings.setSchema(schema);
            return settings;
        }
    }
}
