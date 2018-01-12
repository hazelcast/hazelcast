package com.hazelcast.dataset;

import com.hazelcast.dataset.impl.DataSetService;
import com.hazelcast.dataset.impl.entryprocessor.EpOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.List;
import java.util.Map;

public class CompiledEntryProcessor<V> {
    private final OperationService operationService;
    private final String compileId;
    private final String name;

    public CompiledEntryProcessor(OperationService operationService, String name, String compileId) {
        this.operationService = operationService;
        this.name = name;
        this.compileId = compileId;
    }

    public List<V> execute(Map<String, Object> bindings) {
        try {
            Map<Integer, Object> r = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new EpOperationFactory(name, compileId, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
