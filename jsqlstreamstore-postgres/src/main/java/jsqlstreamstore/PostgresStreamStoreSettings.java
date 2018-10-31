package jsqlstreamstore;

import jsqlstreamstore.infrastructure.Ensure;
import jsqlstreamstore.infrastructure.serialization.JacksonSerializer;
import jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import jsqlstreamstore.subscriptions.PollingStreamStoreNotifier;
import org.joda.time.Period;
import jsqlstreamstore.store.ConnectionFactory;
import jsqlstreamstore.subscriptions.CreateStreamStoreNotifier;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.util.Properties;

// TODO: compare Marten StoreOptions and AdvancedOptions
public class PostgresStreamStoreSettings {

    private static final String DEFAULT_SCHEMA = "public";
    private static final JsonSerializerStrategy DEFAULT_JSON_SERIALIZER_STRATEGY = JacksonSerializer.DEFAULT;
    private static final Period DEFAULT_METADATA_MAX_AGE_CACHE_EXPIRE = Period.minutes(1);
    private static final int DEFAULT_METADATA_MAX_AGE_CACHE_MAX_SIZE = 10000;
    private static final CreateStreamStoreNotifier DEFAULT_CREATE_STREAM_STORE_NOTIFIER = store -> new PollingStreamStoreNotifier(store);

    private String schema = DEFAULT_SCHEMA;
    private ConnectionFactory connectionFactory;
    private JsonSerializerStrategy jsonSerializerStrategy = DEFAULT_JSON_SERIALIZER_STRATEGY;
    private CreateStreamStoreNotifier createStreamStoreNotifier = DEFAULT_CREATE_STREAM_STORE_NOTIFIER;
    private Period metadataMaxAgeCacheExpire = DEFAULT_METADATA_MAX_AGE_CACHE_EXPIRE;
    private int metadataMaxAgeCacheMaxSize = DEFAULT_METADATA_MAX_AGE_CACHE_MAX_SIZE;

    public void setSchema(String schema) {
        Ensure.isNullOrEmpty(schema);
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        Ensure.notNull(connectionFactory);
        this.connectionFactory = connectionFactory;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setJsonSerializerStrategy(JsonSerializerStrategy strategy) {
        Ensure.notNull(strategy);
        this.jsonSerializerStrategy = strategy;
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

    public void setMetadataMaxAgeCacheExpire(Period metadataMaxAgeCacheExpire) {
        this.metadataMaxAgeCacheExpire = metadataMaxAgeCacheExpire;
    }

    public Period getMetadataMaxAgeCacheExpire() {
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
        private Period metadataMaxAgeCacheExpire = DEFAULT_METADATA_MAX_AGE_CACHE_EXPIRE;
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

        public Builder withJsonSerializerStrategry(JsonSerializerStrategy jsonSerializerStrategy) {
            Ensure.notNull(jsonSerializerStrategy);
            this.jsonSerializerStrategy = jsonSerializerStrategy;
            return this;
        }

        public Builder withStreamStoreNotifier(CreateStreamStoreNotifier createStreamStoreNotifier) {
            Ensure.notNull(createStreamStoreNotifier);
            this.createStreamStoreNotifier = createStreamStoreNotifier;
            return this;
        }

        public Builder withMetadataMaxAgeCacheExpire(Period metadataMaxAgeCacheExpire) {
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
