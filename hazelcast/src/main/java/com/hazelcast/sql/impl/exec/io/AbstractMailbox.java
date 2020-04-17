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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.QueryId;

import java.util.UUID;

/**
 * Base class for inboxes and outboxes.
 */
public abstract class AbstractMailbox {
    /** Query ID. */
    protected final QueryId queryId;

    /** Edge ID. */
    protected final int edgeId;

    /** Width of a single row in bytes. */
    protected final int rowWidth;

    /** Local member that is fixed throughout query execution. */
    protected UUID localMemberId;

    public AbstractMailbox(QueryId queryId, int edgeId, int rowWidth, UUID localMemberId) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.rowWidth = rowWidth;
        this.localMemberId = localMemberId;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getRowWidth() {
        return rowWidth;
    }

    public UUID getLocalMemberId() {
        return localMemberId;
    }
}
