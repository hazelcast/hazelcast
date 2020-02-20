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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.impl.QueryFragmentExecutable;
import com.hazelcast.sql.impl.operation.QueryDataExchangeOperation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * State of a distributed query execution.
 */
public class QueryDistributedState {
    /** Pending batches. */
    private final ConcurrentLinkedDeque<QueryDataExchangeOperation> pendingOperations = new ConcurrentLinkedDeque<>();

    /** Lock to prevent conflicts on initialization and batch arrival. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Initialized state. */
    private volatile InitializedState initializedState;

    public Long getTimeout() {
         InitializedState initializedState0 = initializedState;

         return initializedState0 != null ? initializedState0.getTimeout() : null;
    }

    public boolean isStarted() {
        return initializedState != null;
    }

    /**
     * Initialization routine which is called when query start task is executed.
     *
     * @param fragments Fragment executables.
     * @param timeout Timeout.
     */
    public void onStart(List<QueryFragmentExecutable> fragments, long timeout) {
        lock.writeLock().lock();

        try {
            // Initialize the state.
            initializedState = new InitializedState(fragments, timeout);

            // Unwind pending batches if needed.
            boolean hadPendingBatches = false;

            for (QueryDataExchangeOperation pendingOperation : pendingOperations) {
                onOperation0(pendingOperation);

                if (!hadPendingBatches) {
                    hadPendingBatches = true;
                }
            }

            if (hadPendingBatches) {
                pendingOperations.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public QueryFragmentExecutable onOperation(QueryDataExchangeOperation operation) {
        lock.readLock().lock();

        try {
            if (initializedState != null) {
                return onOperation0(operation);
            } else {
                pendingOperations.add(operation);

                return null;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private QueryFragmentExecutable onOperation0(QueryDataExchangeOperation operation) {
        assert initializedState != null;

        QueryFragmentExecutable fragment = initializedState.getFragment(operation.isInbound(), operation.getEdgeId());

        assert fragment != null : operation;

        fragment.addOperation(operation);

        return fragment;
    }

    /**
     * Callback executed when the fragment is finished.
     *
     * @return {@code True} if execution of the last fragment finished.
     */
    public boolean onFragmentFinished() {
        assert initializedState != null;

        return initializedState.onFragmentFinished();
    }

    private static final class InitializedState {
        private final Map<Integer, QueryFragmentExecutable> inboundEdgeToFragment = new HashMap<>();
        private final Map<Integer, QueryFragmentExecutable> outboundEdgeToFragment = new HashMap<>();

        /** Number of remaining fragments. */
        private final AtomicInteger remainingFragmentCount;

        /** Query timeout. */
        private final long timeout;

        private InitializedState(List<QueryFragmentExecutable> fragmentExecutables, long timeout) {
            for (QueryFragmentExecutable fragmentExecutable : fragmentExecutables) {
                for (Integer inboxEdgeId : fragmentExecutable.getInboxEdgeIds()) {
                    QueryFragmentExecutable oldFragmentExecutable = inboundEdgeToFragment.put(inboxEdgeId, fragmentExecutable);

                    assert oldFragmentExecutable == null || fragmentExecutable == oldFragmentExecutable;
                }

                for (Integer outboxEdgeId : fragmentExecutable.getOutboxEdgeIds()) {
                    QueryFragmentExecutable oldFragmentExecutable = outboundEdgeToFragment.put(outboxEdgeId, fragmentExecutable);

                    assert oldFragmentExecutable == null || fragmentExecutable == oldFragmentExecutable;
                }
            }

            this.remainingFragmentCount = new AtomicInteger(fragmentExecutables.size());

            this.timeout = timeout;
        }

        private boolean onFragmentFinished() {
            return remainingFragmentCount.decrementAndGet() == 0;
        }

        private QueryFragmentExecutable getFragment(boolean inbound, int edgeId) {
            if (inbound) {
                return inboundEdgeToFragment.get(edgeId);
            } else {
                return outboundEdgeToFragment.get(edgeId);
            }
        }

        private long getTimeout() {
            return timeout;
        }
    }
}
