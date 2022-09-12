/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.QueryException.cancelledByUser;
import static com.hazelcast.sql.impl.QueryUtils.toPublicException;

class SqlResultImpl extends AbstractSqlResult {

    private final HazelcastInstance hazelcastInstance;
    private final long jobId;
    private final QueryId queryId;
    private final QueryResultProducer rootResultConsumer;
    private final SqlRowMetadata rowMetadata;
    private final boolean isInfiniteRows;

    private ResultIterator<SqlRow> iterator;
    private volatile boolean isClosed;

    SqlResultImpl(
            HazelcastInstance hazelcastInstance,
            long jobId,
            QueryId queryId,
            QueryResultProducer rootResultConsumer,
            SqlRowMetadata rowMetadata,
            boolean isInfiniteRows
    ) {
        this.hazelcastInstance = hazelcastInstance;
        this.jobId = jobId;
        this.queryId = queryId;
        this.rootResultConsumer = rootResultConsumer;
        this.rowMetadata = rowMetadata;
        this.isInfiniteRows = isInfiniteRows;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public boolean isInfiniteRows() {
        return isInfiniteRows;
    }

    @Nonnull @Override
    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    @Nonnull @Override
    public ResultIterator<SqlRow> iterator() {
        if (iterator == null) {
            iterator = new RowToSqlRowIterator(rootResultConsumer.iterator());

            return iterator;
        } else {
            throw new IllegalStateException("Iterator can be requested only once.");
        }
    }

    @Override
    public long updateCount() {
        return -1;
    }

    @Override
    public void close(@Nullable QueryException exception) {
        isClosed = true;
        if (exception != null) {
            rootResultConsumer.onError(exception);
        } else {
            sendJobTermination();
        }
    }

    private void sendJobTermination() {
        rootResultConsumer.done();

        Operation op = new TerminateJobOperation(jobId, TerminationMode.CANCEL_FORCEFUL_QUIET, true);
        getNodeEngine(hazelcastInstance)
            .getOperationService()
            .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, coordinatorId())
            .invoke();
    }

    private Address coordinatorId() {
        NodeEngineImpl container = getNodeEngine(hazelcastInstance);
        return container.getThisAddress();
    }

    private final class RowToSqlRowIterator implements ResultIterator<SqlRow> {

        private final ResultIterator<JetSqlRow> delegate;

        private RowToSqlRowIterator(ResultIterator<JetSqlRow> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            checkNotClosed();
            try {
                return delegate.hasNext();
            } catch (Exception e) {
                throw toPublicException(e, queryId.getMemberId());
            }
        }

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            checkNotClosed();
            try {
                return delegate.hasNext(timeout, timeUnit);
            } catch (Exception e) {
                throw toPublicException(e, queryId.getMemberId());
            }
        }

        @Override
        public SqlRow next() {
            checkNotClosed();
            try {
                return new SqlRowImpl(getRowMetadata(), delegate.next());
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                throw toPublicException(e, queryId.getMemberId());
            }
        }

        private void checkNotClosed() {
            if (isClosed) {
                throw toPublicException(cancelledByUser(), queryId.getMemberId());
            }
        }
    }
}
