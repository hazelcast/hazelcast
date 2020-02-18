/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.worker.QueryWorkerPool;

import java.util.List;

/**
 * Common context for the query.
 */
public class QueryContext {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Worker pool. */
    private final QueryWorkerPool workerPool;

    /** Underlying query state. */
    private final QueryState state;

    /** Query ID. */
    private final QueryId queryId;

    /** Arguments. */
    private final List<Object> arguments;

    public QueryContext(
        NodeEngine nodeEngine,
        QueryWorkerPool workerPool,
        QueryState state,
        QueryId queryId,
        List<Object> arguments
    ) {
        this.nodeEngine = nodeEngine;
        this.workerPool = workerPool;
        this.state = state;
        this.queryId = queryId;
        this.arguments = arguments;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public QueryWorkerPool getWorkerPool() {
        return workerPool;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public void checkCancelled() {
        state.checkCancelled();
    }
}
