package com.hazelcast.dataset;

import com.hazelcast.dataset.impl.DataSetProxy;
import com.hazelcast.dataset.impl.DataSetService;
import com.hazelcast.dataset.impl.projection.NewDataSetOperationFactory;
import com.hazelcast.dataset.impl.projection.ProjectionOperation;
import com.hazelcast.dataset.impl.projection.ProjectionOperationFactory;
import com.hazelcast.dataset.impl.query.QueryOperationFactory;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompiledProjection<K,E> {

    private final OperationService operationService;
    private final String compileId;
    private final String name;
    private final ProjectionRecipe projectRecipe;
    private final NodeEngine nodeEngine;
    private final DataSetService dataSetService;

    public CompiledProjection(OperationService operationService,
                              NodeEngine nodeEngine,
                              String name,
                              String compileId,
                              DataSetService dataSetService,
                              ProjectionRecipe projectionRecipe) {
        this.operationService = operationService;
        this.nodeEngine = nodeEngine;
        this.name = name;
        this.compileId = compileId;
        this.projectRecipe = projectionRecipe;
        this.dataSetService = dataSetService;
    }

    /**
     * Executes the projection and returns the resulting data.
     *
     * @param bindings
     * @return
     */
    public <C extends Collection<E>> C execute(Map<String, Object> bindings, Class<C> collectionClass) {
        try {
            Collection<E> collection = collectionClass.newInstance();

            Map<Integer, Object> r =  operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new ProjectionOperationFactory(
                            name, compileId, bindings, collectionClass));


            for (Object v : r.values()) {
                collection.addAll((Collection) v);
            }
            return (C)collection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new DataSet based on the projection.
     *
     * The big difference between this method and the {@link #execute(Map, Class)} is that the former doesn't download any data,
     * it creates new content at the members and the latter downloads all content.
     *
     * @param dataSetName
     * @param bindings
     * @return
     */
    public DataSet<K,E> newDataSet(String dataSetName, Map<String, Object> bindings) {
        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new NewDataSetOperationFactory(
                            name, dataSetName, projectRecipe.getClassName(), compileId, bindings));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new DataSetProxy(dataSetName, nodeEngine, dataSetService);
    }
}
