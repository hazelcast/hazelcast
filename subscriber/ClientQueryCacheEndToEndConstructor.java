package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapMadePublishableCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapPublisherCreateCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapPublisherCreateWithValueCodec;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.AbstractQueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

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
    public void createPublisherAccumulator(AccumulatorInfo info) throws Exception {
        if (info.isIncludeValue()) {
            createPublishAccumulatorWithIncludeValue(info);
        } else {
            createPublishAccumulatorWithoutIncludeValue(info);
        }
        if (info.isPopulate()) {
            madePublishable(info.getMapName(), info.getCacheName());
        }
    }

    private void createPublishAccumulatorWithIncludeValue(AccumulatorInfo info) {
        Data data = context.getSerializationService().toData(info.getPredicate());
        ClientMessage request = EnterpriseMapPublisherCreateWithValueCodec.encodeRequest(info.getMapName(),
                info.getCacheName(), data,
                info.getBatchSize(), info.getBufferSize(), info.getDelaySeconds(),
                info.isPopulate(), info.isCoalesce());


        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        ClientMessage response = (ClientMessage) invokerWrapper.invoke(request);

        Set<Map.Entry<Data, Data>> result = EnterpriseMapPublisherCreateWithValueCodec.decodeResponse(response).entrySet;

        populateWithValues(queryCache, result);
    }

    private void createPublishAccumulatorWithoutIncludeValue(AccumulatorInfo info) {
        Data data = context.getSerializationService().toData(info.getPredicate());
        ClientMessage request = EnterpriseMapPublisherCreateCodec.encodeRequest(info.getMapName(),
                info.getCacheName(), data,
                info.getBatchSize(), info.getBufferSize(), info.getDelaySeconds(),
                info.isPopulate(), info.isCoalesce());


        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        ClientMessage response = (ClientMessage) invokerWrapper.invoke(request);

        Collection<Data> result = EnterpriseMapPublisherCreateCodec.decodeResponse(response).set;

        populateWithoutValues(queryCache, result);
    }

    private void madePublishable(String mapName, String cacheName) throws Exception {
        ClientMessage request = EnterpriseMapMadePublishableCodec.encodeRequest(mapName, cacheName);
        context.getInvokerWrapper().invokeOnAllPartitions(request);
    }

    private void populateWithValues(InternalQueryCache queryCache, Set<Map.Entry<Data, Data>> result) {
        for (Map.Entry<Data, Data> entry : result) {
            queryCache.setInternal(entry.getKey(), entry.getValue(), false, EntryEventType.ADDED);
        }
    }

    private void populateWithoutValues(InternalQueryCache queryCache, Collection<Data> result) {
        for (Data data : result) {
            queryCache.setInternal(data, null, false, EntryEventType.ADDED);
        }
    }

}
