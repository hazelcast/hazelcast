package com.hazelcast.dataset;

import com.hazelcast.dataset.impl.DataSetService;
import com.hazelcast.dataset.impl.query.QueryOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CompiledPredicate<V> {

    private final OperationService operationService;
    private final String compileId;
    private final String name;

    public CompiledPredicate(OperationService operationService, String name, String compileId) {
        this.operationService = operationService;
        this.name = name;
        this.compileId = compileId;
    }

    public List<V> execute(Map<String, Object> bindings) {
        try {
            List<V> result = new LinkedList<V>();
            Map<Integer, Object> r = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new QueryOperationFactory(name, compileId, bindings));

            for (Object v : r.values()) {
                result.addAll((List) v);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
