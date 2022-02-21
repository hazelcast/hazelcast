/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.SortingUtil;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.query.impl.predicates.PredicateUtils.unwrapPagingPredicate;

public abstract class AbstractMapQueryWithPagingPredicateMessageTask<P> extends DefaultMapQueryMessageTask<P> {
    private Predicate predicate;

    protected AbstractMapQueryWithPagingPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object reduce(Collection<QueryResultRow> result) {
        ArrayList<QueryableEntry> accumulatedList = new ArrayList<>(result.size());

        // TODO: The following lines will be replaced by k-way merge sort algorithm as described at
        //  https://github.com/hazelcast/hazelcast/issues/12205
        result.forEach(
                row -> accumulatedList.add(new CachedQueryEntry(serializationService, row.getKey(), row.getValue(), null)));

        PagingPredicateImpl pagingPredicateImpl = getPagingPredicate();

        List pageData = SortingUtil.getSortedSubListData(accumulatedList, pagingPredicateImpl);

        return new AbstractMap.SimpleImmutableEntry(getPagingPredicate().getAnchorList(), pageData);
    }

    @Override
    protected Predicate getPredicate() {
        if (predicate == null) {
            predicate = getPagingPredicateHolder().asPredicate(serializationService);
        }
        return predicate;
    }

    protected PagingPredicateImpl getPagingPredicate() {
        return unwrapPagingPredicate(getPredicate());
    }

    protected abstract PagingPredicateHolder getPagingPredicateHolder();

}
