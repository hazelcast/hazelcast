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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.aggregation.impl.CanonicalizingHashSet;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.query.AggregationResult;
import com.hazelcast.nio.Connection;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.util.IterationType;

import java.util.Collection;
import java.util.HashSet;

public abstract class DefaultMapAggregateMessageTask<P>
        extends AbstractMapQueryMessageTask<P, AggregationResult, AggregationResult, Object> {

    private static final int MIXED_TYPES_VERSION = BuildInfo.calculateVersion("3.12");

    public DefaultMapAggregateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected IterationType getIterationType() {
        return IterationType.ENTRY;
    }

    @Override
    protected Projection<?, ?> getProjection() {
        return null;
    }

    @Override
    protected Predicate getPredicate() {
        return TruePredicate.INSTANCE;
    }

    @Override
    protected void extractAndAppendResult(Collection<AggregationResult> results, AggregationResult aggregationResult) {
        results.add(aggregationResult);
    }

    @SuppressWarnings({"unchecked", "checkstyle:npathcomplexity"})
    @Override
    protected Object reduce(Collection<AggregationResult> results) {
        if (results.isEmpty()) {
            return null;
        }

        AggregationResult combinedResult = null;
        try {
            for (AggregationResult result : results) {
                if (combinedResult == null) {
                    combinedResult = result;
                } else {
                    combinedResult.combine(result);
                }
            }
        } finally {
            if (combinedResult != null) {
                combinedResult.onCombineFinished();
            }
        }

        if (combinedResult == null) {
            return null;
        }

        Object result = combinedResult.getAggregator().aggregate();
        if (result instanceof CanonicalizingHashSet && endpoint.getClientVersion() < MIXED_TYPES_VERSION) {
            // XXX: Older Java clients are expecting a HashSet instance
            // serialized using standard java.io.Serializable facilities.
            return new HashSet((CanonicalizingHashSet) result);
        }

        return result;
    }

}
