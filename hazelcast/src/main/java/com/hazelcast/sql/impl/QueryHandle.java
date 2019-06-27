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

package com.hazelcast.sql.impl;

/**
 * Handle to running query.
 */
public class QueryHandle {
    /** Query ID. */
    private final QueryId queryId;

    /** Consumer. */
    private final QueryResultConsumer consumer;

    public QueryHandle(QueryId queryId, QueryResultConsumer consumer) {
        this.queryId = queryId;
        this.consumer = consumer;
    }

    /**
     * @return Query ID.
     */
    public QueryId getQueryId() {
        return queryId;
    }

    /**
     * Close the handle.
     */
    public void close() {
        // TODO: Cancel/close support.
    }

    public QueryResultConsumer getConsumer() {
        return consumer;
    }
}
