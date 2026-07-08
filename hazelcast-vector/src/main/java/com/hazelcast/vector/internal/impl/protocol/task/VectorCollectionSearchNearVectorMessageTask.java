/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSearchNearVectorCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAsyncMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.internal.impl.VectorCollectionService;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.client.impl.protocol.codec.builtin.CustomTypeFactory.toVectorValues;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static com.hazelcast.vector.internal.impl.VectorCollectionService.SERVICE_NAME;

public class VectorCollectionSearchNearVectorMessageTask
    extends AbstractAsyncMessageTask<VectorCollectionSearchNearVectorCodec.RequestParameters, SearchResults<Data, Data>> {

    private transient long startTimeNanos;

    VectorCollectionSearchNearVectorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected VectorCollectionSearchNearVectorCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return VectorCollectionSearchNearVectorCodec.decodeRequest(clientMessage);
    }

    @Override
    protected void beforeProcess() {
        startTimeNanos = Timer.nanos();
    }

    @Override
    protected Object processResponseBeforeSending(SearchResults<Data, Data> response) {
        VectorCollectionService service = getService(SERVICE_NAME);
        service.getStatistics(getDistributedObjectName())
                .incrementSearchLatencyNanos(response.size(), Timer.nanosElapsed(startTimeNanos));
        return response;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        // The response at this stage must contain/produce serialized results (DataSearchResult).
        var results = (SearchResults<Data, Data>) response;
        // note that iterator may be single pass but encodeResponse needs it only once,
        // but the lambda we use here does not conform to general Iterable contract.
        return VectorCollectionSearchNearVectorCodec.encodeResponse(() -> results.results());
    }

    @Override
    protected CompletableFuture<SearchResults<Data, Data>> processInternal() {
        VectorCollectionService service = getService(SERVICE_NAME);
        return service.getSearcher(parameters.name, parameters.options)
                .search(parameters.name, toVectorValues(parameters.vectors), parameters.options, endpoint);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SEARCH;
    }

    @Override
    public Permission getRequiredPermission() {
        return new VectorCollectionPermission(getDistributedObjectName(), ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{toVectorValues(parameters.vectors), parameters.options};
    }
}
