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
import java.util.ArrayList;
import java.util.Collection;

/**
 * Common super-class for check operations.
 */
public abstract class QueryAbstractCheckOperation extends QueryOperation {

    private Collection<QueryId> queryIds;

    public QueryAbstractCheckOperation() {
        // No-op.
    }

    public QueryAbstractCheckOperation(Collection<QueryId> queryIds) {
        assert queryIds != null;

        this.queryIds = queryIds;
    }

    public Collection<QueryId> getQueryIds() {
        return queryIds;
    }

    @Override
    protected final void writeInternal0(ObjectDataOutput out) throws IOException {
        out.writeInt(queryIds.size());

        for (QueryId queryId : queryIds) {
            queryId.writeData(out);
        }
    }

    @Override
    protected final void readInternal0(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        queryIds = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            QueryId queryId = new QueryId();

            queryId.readData(in);

            queryIds.add(queryId);
        }
    }
}
