/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.ExceptionUtil;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.ContinuousQueryMessageType#CONTINUOUSQUERY_PUBLISHERCREATEWITHVALUE}
 */
public class MapPublisherCreateWithValueMessageTask
        extends AbstractCallableMessageTask<ContinuousQueryPublisherCreateWithValueCodec.RequestParameters> {

    public MapPublisherCreateWithValueMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ClusterService clusterService = clientEngine.getClusterService();
        Collection<MemberImpl> members = clusterService.getMemberImpls();
        List<Future> futures = new ArrayList<Future>(members.size());
        createInvocations(members, futures);

        return getQueryResults(futures);
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future> futures) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        final ClientEndpoint endpoint = getEndpoint();
        for (MemberImpl member : members) {
            Predicate predicate = serializationService.toObject(parameters.predicate);
            AccumulatorInfo accumulatorInfo =
                    AccumulatorInfo.createAccumulatorInfo(parameters.mapName, parameters.cacheName, predicate,
                            parameters.batchSize, parameters.bufferSize, parameters.delaySeconds,
                            true, parameters.populate, parameters.coalesce);


            PublisherCreateOperation operation = new PublisherCreateOperation(accumulatorInfo);
            operation.setCallerUuid(endpoint.getUuid());
            Address address = member.getAddress();
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, address);
            Future future = invocationBuilder.invoke();
            futures.add(future);
        }
    }

    private Set<Map.Entry<Data, Data>> getQueryResults(List<Future> futures) {
        Map<Data, Data> results = createHashMap(futures.size());
        for (Future future : futures) {
            Object result = null;
            try {
                result = future.get();
            } catch (Throwable t) {
                ExceptionUtil.rethrow(t);
            }
            if (result == null) {
                continue;
            }
            for (QueryResultRow row : (QueryResult) result) {
                results.put(row.getKey(), row.getValue());
            }
        }
        return results.entrySet();
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
        return null;
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
