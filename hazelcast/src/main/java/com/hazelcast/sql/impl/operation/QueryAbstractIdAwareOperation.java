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
 * Base class for query operations which has query ID.
 */
public abstract class QueryAbstractIdAwareOperation extends QueryOperation {

    protected QueryId queryId;

    protected QueryAbstractIdAwareOperation() {
        // No-op.
    }

    protected QueryAbstractIdAwareOperation(QueryId queryId) {
        assert queryId != null;

        this.queryId = queryId;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public int getPartition() {
        return getPartitionForHash(queryId.hashCode());
    }

    @Override
    protected final void writeInternal0(ObjectDataOutput out) throws IOException {
        queryId.writeData(out);

        writeInternal1(out);
    }

    @Override
    protected final void readInternal0(ObjectDataInput in) throws IOException {
        queryId = new QueryId();
        queryId.readData(in);

        try {
            readInternal1(in);
        } catch (Exception e) {
            throw new QueryOperationDeserializationException(queryId, getCallerId(), getClass().getSimpleName(), e);
        }
    }

    protected abstract void writeInternal1(ObjectDataOutput out) throws IOException;
    protected abstract void readInternal1(ObjectDataInput in) throws IOException;
}
