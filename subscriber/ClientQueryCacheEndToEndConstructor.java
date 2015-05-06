package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.client.MadePublishableRequest;
import com.hazelcast.map.impl.client.PublisherCreateRequest;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.subscriber.AbstractQueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.QueryResultSet;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;

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
        PublisherCreateRequest publisherCreateRequest = new PublisherCreateRequest(info);
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        List<QueryResultSet> queryResultSets
                = (List<QueryResultSet>) invokerWrapper.invoke(publisherCreateRequest);
        setResults(queryCache, queryResultSets);
        if (info.isPopulate()) {
            madePublishable(info.getMapName(), info.getCacheName());
        }
    }

    private void madePublishable(String mapName, String cacheName) throws Exception {
        context.getInvokerWrapper().invokeOnAllPartitions(new MadePublishableRequest(mapName, cacheName));
    }

    private void setResults(InternalQueryCache queryCache, List<QueryResultSet> queryResultSets) {
        if (includeValue) {
            populateWithValues(queryCache, queryResultSets);
        } else {
            populateWithoutValues(queryCache, queryResultSets);
        }
    }

    private void populateWithValues(InternalQueryCache queryCache, List<QueryResultSet> queryResultSets) {
        for (QueryResultSet resultSet : queryResultSets) {
            if (resultSet == null || resultSet.isEmpty()) {
                continue;
            }
            Iterator iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                AbstractMap.SimpleImmutableEntry entry = (AbstractMap.SimpleImmutableEntry) iterator.next();
                Data dataKey = (Data) entry.getKey();
                Data valueData = (Data) entry.getValue();
                queryCache.setInternal(dataKey, valueData, false, EntryEventType.ADDED);
            }
        }
    }

    private void populateWithoutValues(InternalQueryCache queryCache, List<QueryResultSet> queryResultSets) {
        for (QueryResultSet resultSet : queryResultSets) {
            if (resultSet == null || resultSet.isEmpty()) {
                continue;
            }
            Iterator iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                Data dataKey = (Data) iterator.next();
                queryCache.setInternal(dataKey, null, false, EntryEventType.ADDED);
            }
        }
    }

}
