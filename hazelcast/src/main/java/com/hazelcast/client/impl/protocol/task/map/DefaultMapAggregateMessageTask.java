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
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.query.AggregationResult;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.internal.util.IterationType;

import java.util.Collection;

public abstract class DefaultMapAggregateMessageTask<P>
        extends AbstractMapQueryMessageTask<P, AggregationResult, AggregationResult, Object> {

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
        return Predicates.alwaysTrue();
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

        return combinedResult.getAggregator().aggregate();
    }

}
