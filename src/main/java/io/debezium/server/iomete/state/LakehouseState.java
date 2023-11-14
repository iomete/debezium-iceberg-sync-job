package io.debezium.server.iomete.state;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

class LakehouseState {
    private static final Logger logger = LoggerFactory.getLogger(LakehouseState.class);
    private static final StructType SCHEMA =
            StructType.fromDDL("offsets string, database_history string");

    private boolean running = false;

    private SparkSession sparkSession;
    private String destinationDatabase;
    private String serverId;

    private Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
    private List<HistoryRecord> historyRecords = new ArrayList<>();


    public LakehouseState(SparkSession sparkSession, String destinationDatabase, String serverId) {
        this.sparkSession = sparkSession;
        this.destinationDatabase = destinationDatabase;
        this.serverId = serverId;

        logger.info("LakehouseState with destinationDatabase: {}, serverId: {}", destinationDatabase, serverId);
    }

    private String tableName() {
        return format("%s.debezium_state_%s", destinationDatabase, serverId);
    }


    void addHistoryRecord(HistoryRecord historyRecord) {
        historyRecords.add(historyRecord);
    }

    // idempotent, safe to call multiple times
    synchronized void start() {
        if (this.running) {
            return;
        }

        this.running = true;
        createTableIfNotExists();
        loadState();
    }

    synchronized void stop() {
        this.running = false;
    }

    void loadState() {
        try {
            var df = this.sparkSession.table(tableName()).limit(1);
            var rows = df.collectAsList();
            if (rows.isEmpty()) {
                return;
            }

            String offsets = rows.get(0).getString(0);
            if (offsets != null && !offsets.isEmpty()) {
                this.offsets = OffsetHelper.fromBase64String(offsets);
            }

            String currentHistory = rows.get(0).getString(1);
            if (currentHistory != null && !currentHistory.isEmpty()) {
                this.historyRecords = HistoryStateHelper.fromDocumentString(currentHistory);
            }
        } catch (Exception analysisException) {
            throw new RuntimeException("Error while loading offsets from state table", analysisException);
        }
    }

    Map<ByteBuffer, ByteBuffer> getOffsets() {
        return this.offsets;
    }

    List<HistoryRecord> getHistory() {
        return this.historyRecords;
    }

    // We also commit history records to the state table when we commit offsets.
    // It will make sure history records and offsets are in sync.
    void saveOffsets(Map<ByteBuffer, ByteBuffer> offsets) throws IOException {
        var offsetBase64 = OffsetHelper.toBase64String(offsets);
        var historyRecordsDocumentString = HistoryStateHelper.toDocumentString(historyRecords);

        var rows = List.of(RowFactory.create(offsetBase64, historyRecordsDocumentString));
        var df = sparkSession.createDataFrame(rows, SCHEMA);
        df.write().mode(SaveMode.Overwrite).saveAsTable(tableName());
    }


    private void createTableIfNotExists() {
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("write.metadata.delete-after-commit.enabled", "true");
        tableProperties.put("write.metadata.previous-versions-max", "2");
        tableProperties.put("history.expire.max-snapshot-age-ms", "3600000"); //1 hour
        tableProperties.put("history.expire.min-snapshots-to-keep", "3");

        String tablePropertiesString = tableProperties.entrySet().stream()
                .map(entry -> format("'%s' = '%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));

        String createTableSql = format(
                "CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg TBLPROPERTIES (%s)",
                tableName(), SCHEMA.toDDL(), tablePropertiesString);

        sparkSession.sql(createTableSql);
    }
}


class OffsetHelper {
    static Map<ByteBuffer, ByteBuffer> fromBase64String(String offsetBase64) {
        Map<ByteBuffer, ByteBuffer> offsetData = new HashMap<>();

        byte[] decodedString = Base64.getDecoder().decode(offsetBase64);
        try (SafeObjectInputStream is = new SafeObjectInputStream(new ByteArrayInputStream(decodedString))) {
            Object obj = is.readObject();
            if (!(obj instanceof HashMap))
                throw new ConnectException("Expected HashMap but found " + obj.getClass());
            Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
            for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
                ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                offsetData.put(key, value);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new ConnectException(e);
        }

        return offsetData;
    }

    static String toBase64String(Map<ByteBuffer, ByteBuffer> offsetData) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        //take snapshot of offset store
        try (ObjectOutputStream os = new ObjectOutputStream(byteArrayOutputStream)) {
            Map<byte[], byte[]> raw = new HashMap<>();
            for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : offsetData.entrySet()) {
                byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                raw.put(key, value);
            }
            os.writeObject(raw);
        } catch (IOException ignored) {
        }

        return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    }
}

class HistoryStateHelper {
    private static final DocumentWriter writer = DocumentWriter.defaultWriter();
    private static final DocumentReader reader = DocumentReader.defaultReader();

    static List<HistoryRecord> fromDocumentString(String documentString) {
        List<HistoryRecord> historyRecords = new ArrayList<>();

        try (Scanner scanner = new Scanner(documentString)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line != null && !line.isEmpty()) {
                    historyRecords.add(new HistoryRecord(reader.read(line)));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return historyRecords;
    }

    static String toDocumentString(List<HistoryRecord> historyRecords) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        for (HistoryRecord historyRecord : historyRecords) {
            String line = writer.write(historyRecord.document());
            stringBuilder.append(line);
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}