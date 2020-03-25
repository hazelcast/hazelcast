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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;

import java.io.IOException;

/**
 * Common class for data exchange operations.
 */
public abstract class QueryAbstractExchangeOperation extends QueryAbstractIdAwareOperation {

    protected int edgeId;

    public QueryAbstractExchangeOperation() {
        // No-op.
    }

    public QueryAbstractExchangeOperation(QueryId queryId, int edgeId) {
        super(queryId);

        this.edgeId = edgeId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    /**
     * @return Whether this is an inbound operation, i.e. it is sent from outbox to inbox.
     */
    public abstract boolean isInbound();

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public int getPartition() {
        return getPartitionForHash(31 * queryId.hashCode() + edgeId);
    }

    @Override
    protected final void writeInternal1(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);

        writeInternal2(out);
    }

    @Override
    protected final void readInternal1(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();

        readInternal2(in);
    }

    protected abstract void writeInternal2(ObjectDataOutput out) throws IOException;
    protected abstract void readInternal2(ObjectDataInput in) throws IOException;
}
