package io.debezium.server.iomete.state;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import io.debezium.server.iomete.SparkSessionProvider;
import javax.enterprise.inject.spi.CDI;

class LakehouseStateProvider {
    private static LakehouseState instance;

    public static synchronized LakehouseState instance() {
        if (instance == null) {
            Config config = ConfigProvider.getConfig();

            String destinationDatabase =
                    config.getValue("debezium.sink.iomete.destination.database", String.class);
            String serverId = config.getValue("debezium.source.database.server.id", String.class);

            instance = new LakehouseState(
                    SparkSessionProvider.sparkSession(),
                    destinationDatabase,
                    serverId
            );
        }
        return instance;
    }
}