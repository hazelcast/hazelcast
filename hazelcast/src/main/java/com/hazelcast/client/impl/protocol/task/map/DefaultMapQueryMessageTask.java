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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.projection.Projection;

import java.util.Collection;

public abstract class DefaultMapQueryMessageTask<P>
        extends AbstractMapQueryMessageTask<P, QueryResult, QueryResultRow, Object> {

    protected DefaultMapQueryMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Aggregator<?, ?> getAggregator() {
        return null;
    }

    @Override
    protected Projection<?, ?> getProjection() {
        return null;
    }

    @Override
    protected void extractAndAppendResult(Collection<QueryResultRow> results, QueryResult queryResult) {
        results.addAll(queryResult.getRows());
    }
}
