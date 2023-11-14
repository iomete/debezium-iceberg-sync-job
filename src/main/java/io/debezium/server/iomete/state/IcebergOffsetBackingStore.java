package io.debezium.server.iomete.state;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

@ApplicationScoped
public class IcebergOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(FileOffsetBackingStore.class);

    private LakehouseState lakehouseState;

    public IcebergOffsetBackingStore() {}

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
    }

    @Override
    public synchronized void start() {
        super.start();
        this.lakehouseState = LakehouseStateProvider.instance();
        this.lakehouseState.start();
        log.info("Starting IcebergOffsetBackingStore");
        load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        lakehouseState.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        log.info("Stopped IcebergOffsetBackingStore");
    }

    private void load() {
        this.data = lakehouseState.getOffsets();
    }

    @Override
    protected void save() {
        try {
            lakehouseState.saveOffsets(this.data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

