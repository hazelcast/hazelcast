package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.client.MadePublishableRequest;
import com.hazelcast.map.impl.client.PublisherCreateRequest;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.AbstractQueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;

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
        List<QueryResult> queryResults
                = (List<QueryResult>) invokerWrapper.invoke(publisherCreateRequest);
        setResults(queryCache, queryResults);
        if (info.isPopulate()) {
            madePublishable(info.getMapName(), info.getCacheName());
        }
    }

    private void madePublishable(String mapName, String cacheName) throws Exception {
        context.getInvokerWrapper().invokeOnAllPartitions(new MadePublishableRequest(mapName, cacheName));
    }

    private void setResults(InternalQueryCache queryCache, List<QueryResult> queryResults) {
        // afaik we can get
        if (includeValue) {
            populateWithValues(queryCache, queryResults);
        } else {
            populateWithoutValues(queryCache, queryResults);
        }
    }

    private void populateWithValues(InternalQueryCache queryCache, List<QueryResult> queryResults) {
        for (QueryResult queryResult : queryResults) {
            if (queryResult == null || queryResult.isEmpty()) {
                continue;
            }
            for (QueryResultRow row : queryResult) {
                queryCache.setInternal(row.getKey(), row.getValue(), false, EntryEventType.ADDED);
            }
        }
    }

    private void populateWithoutValues(InternalQueryCache queryCache, List<QueryResult> queryResultSets) {
        for (QueryResult queryResult : queryResultSets) {
            if (queryResult == null || queryResult.isEmpty()) {
                continue;
            }
            for (QueryResultRow row : queryResult) {
                queryCache.setInternal(row.getKey(), null, false, EntryEventType.ADDED);
            }
        }
    }
}
