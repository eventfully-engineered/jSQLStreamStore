package com.eventfullyengineered.jsqlstreamstore.sqlite;

import com.eventfullyengineered.jsqlstreamstore.infrastructure.Ensure;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.serialization.JacksonSerializer;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import com.eventfullyengineered.jsqlstreamstore.store.ConnectionFactory;

import java.sql.DriverManager;

public class SqliteStreamStoreSettings {

    private static final JsonSerializerStrategy DEFAULT_JSON_SERIALIZER_STRATEGY = JacksonSerializer.DEFAULT;

    private ConnectionFactory connectionFactory;
    private JsonSerializerStrategy jsonSerializerStrategy = DEFAULT_JSON_SERIALIZER_STRATEGY;

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

    public static class Builder {

        private ConnectionFactory connectionFactory;
        private JsonSerializerStrategy jsonSerializerStrategy = DEFAULT_JSON_SERIALIZER_STRATEGY;

        public Builder(final String url) {
            this(() -> DriverManager.getConnection(url));
        }

        public Builder(ConnectionFactory connectionFactory) {
            Ensure.notNull(connectionFactory);
            this.connectionFactory = connectionFactory;
        }

        public Builder withJsonSerializerStrategy(JsonSerializerStrategy jsonSerializerStrategy) {
            Ensure.notNull(jsonSerializerStrategy);
            this.jsonSerializerStrategy = jsonSerializerStrategy;
            return this;
        }

        public SqliteStreamStoreSettings build() {
            SqliteStreamStoreSettings settings = new SqliteStreamStoreSettings();
            settings.setConnectionFactory(connectionFactory);
            settings.setJsonSerializerStrategy(jsonSerializerStrategy);
            return settings;
        }
    }
}
