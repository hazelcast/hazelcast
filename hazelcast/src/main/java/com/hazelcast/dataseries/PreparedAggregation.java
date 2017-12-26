package com.hazelcast.dataseries;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataseries.impl.DataSeriesService;
import com.hazelcast.dataseries.impl.aggregation.ExecuteAggregationOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;

public class PreparedAggregation<E> {

    private final OperationService operationService;
    private final String preparedId;
    private final String name;

    public PreparedAggregation(OperationService operationService,
                               String name,
                               String preparedId) {
        this.operationService = operationService;
        this.name = name;
        this.preparedId = preparedId;
    }

    /**
     * Executes the whole aggregation on the partition thread (can be very problematic with large dataseries)
     *
     * @param bindings
     * @return
     */
    public E executePartitionThread(Map<String, Object> bindings) {
        return execute(bindings, false);
    }

    /**
     * Executes the aggregation on forjoin pool. Only the eden segment is processed on partition thread, but
     * all the tenured segments will be processed on the forkjoin pool.
     *
     * @param bindings
     * @return
     */
    public E executeForkJoin(Map<String, Object> bindings) {
        return execute(bindings, true);
    }

    private E execute(Map<String, Object> bindings, boolean forkJoin) {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new ExecuteAggregationOperationFactory(name, preparedId, bindings, forkJoin));

            Aggregator resultAggregator = null;
            for (Object v : result.values()) {
                Aggregator a = (Aggregator) v;
                if (resultAggregator == null) {
                    resultAggregator = a;
                } else {
                    resultAggregator.combine(a);
                }
            }

            resultAggregator.onCombinationFinished();
            return (E) resultAggregator.aggregate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
