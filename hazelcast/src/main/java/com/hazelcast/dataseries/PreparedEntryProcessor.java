package com.hazelcast.dataseries;

import com.hazelcast.dataseries.impl.DataSeriesService;
import com.hazelcast.dataseries.impl.entryprocessor.ExecuteEntryProcessorOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.List;
import java.util.Map;

public class PreparedEntryProcessor<V> {
    private final OperationService operationService;
    private final String preparedId;
    private final String name;

    public PreparedEntryProcessor(OperationService operationService, String name, String preparedId) {
        this.operationService = operationService;
        this.name = name;
        this.preparedId = preparedId;
    }

    public List<V> execute(Map<String, Object> bindings) {
        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new ExecuteEntryProcessorOperationFactory(name, preparedId, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
