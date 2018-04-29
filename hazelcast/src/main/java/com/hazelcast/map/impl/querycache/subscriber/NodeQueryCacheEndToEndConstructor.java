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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Node-side implementation of {@code QueryCacheEndToEndConstructor}.
 *
 * @see QueryCacheEndToEndConstructor
 */
public class NodeQueryCacheEndToEndConstructor extends AbstractQueryCacheEndToEndConstructor {

    public NodeQueryCacheEndToEndConstructor(QueryCacheRequest request) {
        super(request);
    }

    @Override
    public void createPublisherAccumulator(AccumulatorInfo info) throws Exception {
        // create publishers and execute initial population query in one go
        Collection<QueryResult> results = createPublishersAndGetQueryResults(info);
        setResults(queryCache, results);
        boolean populate = info.isPopulate();

        if (logger.isFinestEnabled()) {
            logger.finest(format("Pre population is %s", populate ? "enabled" : "disabled"));
        }

        if (populate) {
            madePublishable(info.getMapName(), info.getCacheId());
        }
    }

    private Collection<QueryResult> createPublishersAndGetQueryResults(AccumulatorInfo info) {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        Collection<Member> members = context.getMemberList();
        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(members.size());
        for (Member member : members) {
            Address address = member.getAddress();
            Future future = invokerWrapper.invokeOnTarget(new PublisherCreateOperation(info), address);
            futures.add(future);
        }
        return returnWithDeadline(futures, OPERATION_WAIT_TIMEOUT_MINUTES, MINUTES);
    }

    private void madePublishable(String mapName, String cacheId) throws Exception {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();

        Collection<Member> memberList = context.getMemberList();
        List<Future> futures = new ArrayList<Future>(memberList.size());
        for (Member member : memberList) {
            Operation operation = new MadePublishableOperation(mapName, cacheId);
            Future future = invokerWrapper.invokeOnTarget(operation, member.getAddress());
            futures.add(future);
        }

        waitWithDeadline(futures, OPERATION_WAIT_TIMEOUT_MINUTES, MINUTES);
    }

    private void setResults(InternalQueryCache queryCache, Collection<QueryResult> results) {
        if (results == null || results.isEmpty()) {
            return;
        }

        //todo: afaik no switch is needed since queryresults will not contain value if it wasn't requested.
        if (includeValue) {
            populateWithValues(queryCache, results);
        } else {
            populateWithoutValues(queryCache, results);
        }
    }

    private void populateWithValues(InternalQueryCache queryCache, Collection<QueryResult> resultSets) {
        for (QueryResult queryResult : resultSets) {
            try {
                if (queryResult == null || queryResult.isEmpty()) {
                    continue;
                }
                for (QueryResultRow row : queryResult) {
                    Data keyData = row.getKey();
                    Data valueData = row.getValue();
                    queryCache.setInternal(keyData, valueData, false, EntryEventType.ADDED);
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }

    }

    private void populateWithoutValues(InternalQueryCache queryCache, Collection<QueryResult> resultSets) {
        for (QueryResult queryResult : resultSets) {
            try {
                if (queryResult == null) {
                    continue;
                }
                for (QueryResultRow row : queryResult) {
                    Data dataKey = row.getKey();
                    queryCache.setInternal(dataKey, null, false, EntryEventType.ADDED);
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
}
