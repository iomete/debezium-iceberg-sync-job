package io.debezium.server.iomete.batch;


import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

@ApplicationScoped
@Named("NoBatchSizeWait")
public class NoBatchSizeWait implements InterfaceBatchSizeWait {
    public void waitMs(long numRecordsProcessed, Integer processingTimeMs) {
    }
}
