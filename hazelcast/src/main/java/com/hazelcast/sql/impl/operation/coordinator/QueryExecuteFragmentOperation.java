/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation.coordinator;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.worker.QueryFragmentExecutable;

/**
 * Special operation to trigger execution of a concrete fragment.
 * <p>
 * This operation is always local and created when the {@link QueryExecuteOperation} contains several fragments that should be
 * executed in parallel.
 */
public class QueryExecuteFragmentOperation extends QueryOperation {

    private QueryFragmentExecutable fragment;

    @SuppressWarnings("unused")
    public QueryExecuteFragmentOperation() {
        // No-op
    }

    public QueryExecuteFragmentOperation(QueryFragmentExecutable fragment) {
        this.fragment = fragment;
    }

    public QueryFragmentExecutable getFragment() {
        return fragment;
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException("Should not be called (operation is always local)");
    }

    @Override
    protected void writeInternal0(ObjectDataOutput out) {
        throw new UnsupportedOperationException("Should not be called (operation is always local)");
    }

    @Override
    protected void readInternal0(ObjectDataInput in) {
        throw new UnsupportedOperationException("Should not be called (operation is always local)");
    }
}
