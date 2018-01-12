package com.hazelcast.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataset.impl.DataSetService;
import com.hazelcast.dataset.impl.aggregation.AggregateOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;

public class CompiledAggregation<E> {

    private final OperationService operationService;
    private final String compileId;
    private final String name;

    public CompiledAggregation(OperationService operationService, String name, String compileId) {
        this.operationService = operationService;
        this.name = name;
        this.compileId = compileId;
    }

    public E execute(Map<String, Object> bindings) {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new AggregateOperationFactory(name, compileId, bindings));

            Aggregator aggregator = null;
            for (Object v : result.values()) {
                Aggregator a = (Aggregator) v;
                if (aggregator == null) {
                    aggregator = a;
                } else {
                    aggregator.combine(a);
                }
            }

            aggregator.onCombinationFinished();
            return (E) aggregator.aggregate();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
