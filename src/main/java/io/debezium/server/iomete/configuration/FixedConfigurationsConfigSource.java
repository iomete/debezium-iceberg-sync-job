package io.debezium.server.iomete.configuration;

import org.eclipse.microprofile.config.spi.ConfigSource;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class FixedConfigurationsConfigSource implements ConfigSource {
    private static final Map<String, String> configuration = new HashMap<>();

    static {
        configuration.put("debezium.sink.type", "iomete");

        configuration.put("debezium.source.decimal.handling.mode", "double");
        configuration.put("debezium.source.include.schema.changes", "false");

        // State
        configuration.put("debezium.source.schema.history.internal", "io.debezium.server.iomete.state.IometeSchemaHistory");
        configuration.put("debezium.source.offset.storage", "io.debezium.server.iomete.state.IcebergOffsetBackingStore");
        configuration.put("debezium.source.offset.flush.interval.ms", "0"); //Always commit offset to avoid reprocessing
        configuration.put("debezium.source.offset.flush.timeout.ms", "10000");

        // Destination
        configuration.put("debezium.sink.batch.batch-size-wait", "MaxBatchSizeWait");
    }

    @Override
    public int getOrdinal() {
        // higher than even system properties. See: https://quarkus.io/guides/config-reference#application-properties-file
        return 500;
    }

    @Override
    public Set<String> getPropertyNames() {
        return configuration.keySet();
    }

    @Override
    public String getValue(final String propertyName) {
        return configuration.get(propertyName);
    }

    @Override
    public String getName() {
        return FixedConfigurationsConfigSource.class.getSimpleName();
    }
}
