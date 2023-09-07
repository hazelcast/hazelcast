/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

public final class UpdateSqlResultImpl extends AbstractSqlResult {

    private final long updateCount;
    private final int partitionArgumentIndex;
    private final long jobId;

    private UpdateSqlResultImpl(long updateCount, int partitionArgumentIndex, long jobId) {
        this.updateCount = checkNotNegative(updateCount, "the updateCount must be >= 0");
        this.partitionArgumentIndex = partitionArgumentIndex;
        this.jobId = jobId;
    }

    public static UpdateSqlResultImpl createUpdateCountResult(long updateCount) {
        return createUpdateCountResult(updateCount, -1);
    }

    public static UpdateSqlResultImpl createUpdateCountResult(long updateCount, int partitionArgumentIndex) {
        return createUpdateCountResult(updateCount, partitionArgumentIndex, -1L);
    }

    public static UpdateSqlResultImpl createUpdateCountResult(long updateCount, int partitionArgumentIndex, long jobId) {
        return new UpdateSqlResultImpl(updateCount, partitionArgumentIndex, jobId);
    }

    @Nullable
    public QueryId getQueryId() {
        throw new IllegalStateException("This result contains only update count");
    }

    @Override
    public boolean isInfiniteRows() {
        return false;
    }

    @Override
    public int getPartitionArgumentIndex() {
        return partitionArgumentIndex;
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
    public long jobId() {
        return jobId;
    }

    @Override
    public void close(@Nullable QueryException error) {
    }
}
