package com.hazelcast.dataseries;

import com.hazelcast.dataseries.impl.DataSeriesService;
import com.hazelcast.dataseries.impl.query.ExecuteQueryOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PreparedQuery<V> {

    private final OperationService operationService;
    private final String preparedId;
    private final String name;

    public PreparedQuery(OperationService operationService, String name, String preparedId) {
        this.operationService = operationService;
        this.name = name;
        this.preparedId = preparedId;
    }

    public List<V> execute(Map<String, Object> bindings) {
        try {
            List<V> result = new LinkedList<>();
            Map<Integer, Object> r = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new ExecuteQueryOperationFactory(name, preparedId, bindings));

            for (Object v : r.values()) {
                result.addAll((List) v);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
