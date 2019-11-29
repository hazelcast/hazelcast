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

package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryMadePublishableCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.AbstractQueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.internal.serialization.Data;

/**
 * Client-side implementation of {@code QueryCacheEndToEndConstructor}.
 *
 * @see QueryCacheEndToEndConstructor
 */
public class ClientQueryCacheEndToEndConstructor extends AbstractQueryCacheEndToEndConstructor {

    public ClientQueryCacheEndToEndConstructor(QueryCacheRequest request) {
        super(request);
    }

    @Override
    public void createPublisherAccumulator(AccumulatorInfo info, boolean urgent) throws Exception {
        ClientMessage publisherCreateMessage = newPublisherCreateMessage(info);

        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        ClientMessage response = (ClientMessage) invokerWrapper.invoke(publisherCreateMessage, urgent);

        if (info.isIncludeValue()) {
            ContinuousQueryPublisherCreateWithValueCodec.decodeResponse(response, (keyData, valueData) -> {
                if (!queryCache.reachedMaxCapacity()) {
                    queryCache.prepopulate(keyData, valueData);
                }
            });
        } else {
            ContinuousQueryPublisherCreateCodec.decodeResponse(response, (data) -> {
                if (!queryCache.reachedMaxCapacity()) {
                    queryCache.prepopulate(data, null);
                }
            });
        }


        if (info.isPopulate()) {
            madePublishable(info.getMapName(), info.getCacheId(), urgent);
            info.setPublishable(true);
        }
    }

    private ClientMessage newPublisherCreateMessage(AccumulatorInfo info) {
        Data dataPredicate = context.getSerializationService().toData(info.getPredicate());

        if (info.isIncludeValue()) {
            return ContinuousQueryPublisherCreateWithValueCodec.encodeRequest(info.getMapName(),
                    info.getCacheId(), dataPredicate,
                    info.getBatchSize(), info.getBufferSize(), info.getDelaySeconds(),
                    info.isPopulate(), info.isCoalesce());

        }

        return ContinuousQueryPublisherCreateCodec.encodeRequest(info.getMapName(),
                info.getCacheId(), dataPredicate,
                info.getBatchSize(), info.getBufferSize(), info.getDelaySeconds(),
                info.isPopulate(), info.isCoalesce());
    }

    private void madePublishable(String mapName, String cacheName, boolean urgent) throws Exception {
        ClientMessage request = ContinuousQueryMadePublishableCodec.encodeRequest(mapName, cacheName);
        context.getInvokerWrapper().invokeOnAllPartitions(request, urgent);
    }
}
