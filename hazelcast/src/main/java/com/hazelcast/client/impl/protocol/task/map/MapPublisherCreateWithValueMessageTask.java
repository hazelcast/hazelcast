/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.util.collection.InflatableSet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.util.ExceptionUtil;

import java.security.Permission;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec#REQUEST_MESSAGE_TYPE}
 */
public class MapPublisherCreateWithValueMessageTask
        extends AbstractCallableMessageTask<ContinuousQueryPublisherCreateWithValueCodec.RequestParameters>
        implements BlockingMessageTask {

    public MapPublisherCreateWithValueMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ClusterService clusterService = clientEngine.getClusterService();
        Collection<MemberImpl> members = clusterService.getMemberImpls();
        List<Future> snapshotFutures = NamespaceUtil.callWithNamespace(nodeEngine,
                MapService.lookupNamespace(nodeEngine, parameters.mapName),
                () -> createPublishersAndGetSnapshotOf(members));
        return fetchMapSnapshotFrom(snapshotFutures);
    }

    private List<Future> createPublishersAndGetSnapshotOf(Collection<MemberImpl> members) {
        List<Future> futures = new ArrayList<>(members.size());
        OperationService operationService = nodeEngine.getOperationService();
        for (MemberImpl member : members) {
            Predicate predicate = serializationService.toObject(parameters.predicate);
            AccumulatorInfo accumulatorInfo =
                    AccumulatorInfo.toAccumulatorInfo(parameters.mapName, parameters.cacheName, predicate,
                            parameters.batchSize, parameters.bufferSize, parameters.delaySeconds,
                            true, parameters.populate, parameters.coalesce);


            PublisherCreateOperation operation = new PublisherCreateOperation(accumulatorInfo);
            operation.setCallerUuid(endpoint.getUuid());
            Address address = member.getAddress();
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, address);
            Future future = invocationBuilder.invoke();
            futures.add(future);
        }

        return futures;
    }

    private static Set<Map.Entry<Data, Data>> fetchMapSnapshotFrom(List<Future> futures) {
        List<Object> queryResults = new ArrayList<>(futures.size());
        int queryResultSize = 0;

        for (Future future : futures) {
            Object result;
            try {
                result = future.get();
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
            if (result == null) {
                continue;
            }

            queryResults.add(result);
            queryResultSize += ((QueryResult) result).size();
        }

        return unpackResults(queryResults, queryResultSize);
    }

    private static Set<Map.Entry<Data, Data>> unpackResults(List<Object> results, int numOfEntries) {
        InflatableSet.Builder<Map.Entry<Data, Data>> builder = InflatableSet.newBuilder(numOfEntries);
        for (Object result : results) {
            for (QueryResultRow row : (QueryResult) result) {
                builder.add(new AbstractMap.SimpleEntry<>(row.getKey(), row.getValue()));
            }
        }
        return builder.build();
    }

    @Override
    protected ContinuousQueryPublisherCreateWithValueCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ContinuousQueryPublisherCreateWithValueCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ContinuousQueryPublisherCreateWithValueCodec.encodeResponse((Set<Map.Entry<Data, Data>>) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.mapName, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
