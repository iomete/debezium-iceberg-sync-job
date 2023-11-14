package io.debezium.server.iomete;

import io.debezium.DebeziumException;
import io.debezium.server.iomete.configuration.DestinationConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

@ApplicationScoped
public class TableHandler {
    public static final String PARTITION_COLUMN_NAME = "processing_time";

    private final SparkSession spark;
    private final DestinationConfiguration destinationConfiguration;

    public TableHandler(DestinationConfiguration destinationConfiguration) {
        this.spark = SparkSessionProvider.sparkSession();
        this.destinationConfiguration = destinationConfiguration;
    }

    public static StructType tableSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("source_server", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_topic", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_offset_ts_sec", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("source_offset_file", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_offset_pos", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("source_offset_snapshot", DataTypes.BooleanType, true));
        fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("key", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("processing_time", DataTypes.TimestampType, true));

        return DataTypes.createStructType(fields);
    }

    public void createTableIfNotExists() {
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("write.metadata.delete-after-commit.enabled", "true");
        tableProperties.put("write.metadata.previous-versions-max", "2");
        tableProperties.put("history.expire.max-snapshot-age-ms", "3600000"); //1 hour
        tableProperties.put("history.expire.min-snapshots-to-keep", "3");

        String tablePropertiesString = tableProperties.entrySet().stream()
                .map(entry -> format("'%s' = '%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));

        String createTableSql = format(
                "CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg PARTITIONED BY (days(%s)) TBLPROPERTIES (%s)",
                destinationConfiguration.fullTableName(), tableSchema().toDDL(), PARTITION_COLUMN_NAME, tablePropertiesString);

        spark.sql(createTableSql);
    }


    public void writeToTable(Dataset<Row> dataFrame) {
        var tableName = destinationConfiguration.fullTableName();
        try {
            dataFrame.writeTo(tableName).append();
        } catch (Exception e) {
            throw new DebeziumException(format("Writing to table %s failed", tableName), e);
        }
    }
}
