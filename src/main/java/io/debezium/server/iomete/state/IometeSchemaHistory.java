package io.debezium.server.iomete.state;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.relational.history.*;
import io.debezium.util.FunctionalReadWriteLock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@ThreadSafe
public class IometeSchemaHistory extends AbstractSchemaHistory {
    private LakehouseState lakehouseState;

    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final AtomicBoolean running = new AtomicBoolean();

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
    }

    @Override
    public void start() {
        super.start();
        running.set(true);
        this.lakehouseState = LakehouseStateProvider.instance();
        this.lakehouseState.start();
    }

    @Override
    public void stop() {
        running.set(false);
        super.stop();
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (record == null) {
            return;
        }

        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }
            lakehouseState.addHistoryRecord(record);
        });
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            lakehouseState.getHistory().forEach(records);
        });
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public boolean exists() {
        return !lakehouseState.getHistory().isEmpty();
    }

    @Override
    public String toString() {
        return "IometeSchemaHistory";
    }
}