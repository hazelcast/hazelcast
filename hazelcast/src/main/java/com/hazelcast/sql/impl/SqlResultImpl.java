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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * Cursor implementation.
 */
public final class SqlResultImpl extends AbstractSqlResult {

    private final long updateCount;

    private SqlResultImpl(long updateCount) {
        this.updateCount = checkNotNegative(updateCount, "the updateCount must be >= 0");
    }

    public static SqlResultImpl createUpdateCountResult(long updateCount) {
        checkNotNegative(updateCount, "the updateCount must be >= 0");
        return new SqlResultImpl(updateCount);
    }

    @Nullable
    public QueryId getQueryId() {
        throw new IllegalStateException("This result contains only update count");
    }

    @Override
    public boolean isInfiniteRows() {
        return false;
    }

    @Nonnull
    @Override
    public SqlRowMetadata getRowMetadata() {
        throw new IllegalStateException("This result contains only update count");
    }

    @Nonnull
    @Override
    public ResultIterator<SqlRow> iterator() {
        throw new IllegalStateException("This result contains only update count");
    }

    @Override
    public long updateCount() {
        return updateCount;
    }

    @Override
    public void close(@Nullable QueryException error) {
    }

    @Override
    public Object deserialize(Object value) {
        throw new IllegalStateException("This result contains only update count");
    }

    @Override
    public Object deserialize(LazyTarget value) {
        throw new IllegalStateException("This result contains only update count");
    }
}
