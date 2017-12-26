/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.datastream;

import com.hazelcast.datastream.impl.DSProxy;
import com.hazelcast.datastream.impl.DSService;
import com.hazelcast.datastream.impl.projection.ExecuteProjectionOperationFactory;
import com.hazelcast.datastream.impl.projection.NewDataStreamOperationFactory;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Map;

public class PreparedProjection<E> {

    private final OperationService operationService;
    private final String preparedId;
    private final String name;
    private final ProjectionRecipe projectRecipe;
    private final NodeEngine nodeEngine;
    private final DSService service;

    public PreparedProjection(OperationService operationService,
                              NodeEngine nodeEngine,
                              String name,
                              String preparedId,
                              DSService service,
                              ProjectionRecipe projectionRecipe) {
        this.operationService = operationService;
        this.nodeEngine = nodeEngine;
        this.name = name;
        this.preparedId = preparedId;
        this.projectRecipe = projectionRecipe;
        this.service = service;
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
                    DSService.SERVICE_NAME, new ExecuteProjectionOperationFactory(
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
     * Creates a new DataStream based on the projection.
     * <p>
     * The big difference between this method and the {@link #execute(Map, Class)} is that the former doesn't download any data,
     * it creates new content at the members and the latter downloads all content.
     *
     * @param dataStreamName
     * @param bindings
     * @return
     */
    public DataStream<E> newDataStream(String dataStreamName, Map<String, Object> bindings) {
        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new NewDataStreamOperationFactory(
                            name, dataStreamName, projectRecipe.getClassName(), preparedId, bindings));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new DSProxy(dataStreamName, nodeEngine, service);
    }
}
