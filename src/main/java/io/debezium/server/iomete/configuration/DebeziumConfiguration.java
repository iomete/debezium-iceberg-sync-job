package io.debezium.server.iomete.configuration;


import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.validation.constraints.Max;

@ConfigMapping(prefix = "debezium")
public interface DebeziumConfiguration {
    @WithName("format.value")
    @WithDefault("json")
    String valueFormat();

    @WithName("format.key")
    @WithDefault("json")
    String keyFormat();

    @WithName("sink.batch.batch-size-wait")
    @WithDefault("NoBatchSizeWait")
    String batchSizeWaitName();
}
