package io.debezium.server.iomete;


import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import java.io.File;


public class SparkSessionProvider {
    private static final Logger logger = LoggerFactory.getLogger(SparkSessionProvider.class);
    private final static String DEV_LAKEHOUSE_DIR = ".lakehouse";
    private final static String DEV_PROFILE_NAME = "dev";

    private static SparkSession sparkSession;

    public synchronized static SparkSession sparkSession() {
        Config config = ConfigProvider.getConfig();
        String profileName = config.getValue("quarkus.profile", String.class);
        logger.info("profileName = " + profileName);

        if (sparkSession == null) {
            if (profileName.equals(DEV_PROFILE_NAME)) {
                SparkSessionProvider.sparkSession = devSparkSession();
            } else {
                SparkSessionProvider.sparkSession = prodSparkSession();
            }
        }
        return sparkSession;
    }

    private static SparkSession prodSparkSession(){
        logger.info("Creating prod SparkSession");
        return SparkSession.builder()
                .appName("iomete-debezium")
                .getOrCreate();
    }

    private static SparkSession devSparkSession(){
        logger.info("Creating dev SparkSession");
        File lakehouseDir = new File(DEV_LAKEHOUSE_DIR);
        return SparkSession.builder()
                .appName("iomete-debezium-dev")
                .master("local")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", lakehouseDir.getAbsolutePath())
                .config("spark.sql.warehouse.dir", lakehouseDir.getAbsolutePath())
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .config("spark.sql.sources.default", "iceberg")
                .getOrCreate();
    }
}
