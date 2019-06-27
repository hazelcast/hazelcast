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

package com.hazelcast.sql.impl.mailbox;

import com.hazelcast.internal.query.QueryId;

/**
 * Base class for inboxes and outboxes.
 */
public abstract class AbstractMailbox {
    /** Query ID. */
    protected final QueryId queryId;

    /** Edge ID. */
    protected final int edgeId;
    
    /** Stripe (known in advance). */
    protected final int stripe;
    
    /** Thread (known after prepare). */
    private int thread;

    public AbstractMailbox(QueryId queryId, int edgeId, int stripe) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.stripe = stripe;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getStripe() {
        return stripe;
    }

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }
}
