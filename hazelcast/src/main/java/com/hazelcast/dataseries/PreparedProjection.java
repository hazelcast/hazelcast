package com.hazelcast.dataseries;

import com.hazelcast.dataseries.impl.DataSeriesProxy;
import com.hazelcast.dataseries.impl.DataSeriesService;
import com.hazelcast.dataseries.impl.projection.NewDataSeriesOperationFactory;
import com.hazelcast.dataseries.impl.projection.ExecuteProjectionOperationFactory;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Map;

public class PreparedProjection<K, E> {

    private final OperationService operationService;
    private final String preparedId;
    private final String name;
    private final ProjectionRecipe projectRecipe;
    private final NodeEngine nodeEngine;
    private final DataSeriesService dataSeriesService;

    public PreparedProjection(OperationService operationService,
                              NodeEngine nodeEngine,
                              String name,
                              String preparedId,
                              DataSeriesService dataSeriesService,
                              ProjectionRecipe projectionRecipe) {
        this.operationService = operationService;
        this.nodeEngine = nodeEngine;
        this.name = name;
        this.preparedId = preparedId;
        this.projectRecipe = projectionRecipe;
        this.dataSeriesService = dataSeriesService;
    }

    public <C extends Collection<E>> C executePartitionThread(Map<String, Object> bindings,
                                                              Class<C> collectionClass) {
        return execute(bindings, collectionClass, false);
    }

    public <C extends Collection<E>> C executeForkJoin(Map<String, Object> bindings,
                                                       Class<C> collectionClass) {
        return execute(bindings, collectionClass, true);
    }

    private <C extends Collection<E>> C execute(Map<String, Object> bindings,
                                                Class<C> collectionClass,
                                                boolean forkJoin) {
        try {
            Collection<E> collection = collectionClass.newInstance();

            Map<Integer, Object> r = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new ExecuteProjectionOperationFactory(
                            name, preparedId, bindings, collectionClass, forkJoin));


            for (Object v : r.values()) {
                collection.addAll((Collection) v);
            }
            return (C) collection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new DataSeries based on the projection.
     * <p>
     * The big difference between this method and the {@link #execute(Map, Class)} is that the former doesn't download any data,
     * it creates new content at the members and the latter downloads all content.
     *
     * @param dataSeriesName
     * @param bindings
     * @return
     */
    public DataSeries<K, E> newDataSeries(String dataSeriesName, Map<String, Object> bindings) {
        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new NewDataSeriesOperationFactory(
                            name, dataSeriesName, projectRecipe.getClassName(), preparedId, bindings));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new DataSeriesProxy(dataSeriesName, nodeEngine, dataSeriesService);
    }
}
