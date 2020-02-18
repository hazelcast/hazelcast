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
import com.hazelcast.sql.impl.operation.QueryBatchOperation;

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
    private final ConcurrentLinkedDeque<QueryBatchOperation> pendingBatches = new ConcurrentLinkedDeque<>();

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

            for (QueryBatchOperation pendingBatch : pendingBatches) {
                onBatch0(pendingBatch);

                if (!hadPendingBatches) {
                    hadPendingBatches = true;
                }
            }

            if (hadPendingBatches) {
                pendingBatches.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public QueryFragmentExecutable onBatch(QueryBatchOperation batch) {
        lock.readLock().lock();

        try {
            if (initializedState != null) {
                return onBatch0(batch);
            } else {
                pendingBatches.add(batch);

                return null;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private QueryFragmentExecutable onBatch0(QueryBatchOperation batch) {
        assert initializedState != null;

        QueryFragmentExecutable fragment = initializedState.getFragment(batch.getEdgeId());

        assert fragment != null;

        fragment.addBatch(batch);

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
        /** Inboxes created for the query. */
        private final Map<Integer, QueryFragmentExecutable> inboxEdgeToFragment = new HashMap<>();

        /** Number of remaining fragments. */
        private final AtomicInteger remainingFragmentCount;

        /** Query timeout. */
        private final long timeout;

        private InitializedState(List<QueryFragmentExecutable> fragmentExecutables, long timeout) {
            for (QueryFragmentExecutable fragmentExecutable : fragmentExecutables) {
                for (Integer inboxEdgeId : fragmentExecutable.getInboxEdgeIds()) {
                    inboxEdgeToFragment.putIfAbsent(inboxEdgeId, fragmentExecutable);
                }
            }

            this.remainingFragmentCount = new AtomicInteger(fragmentExecutables.size());

            this.timeout = timeout;
        }

        private boolean onFragmentFinished() {
            return remainingFragmentCount.decrementAndGet() == 0;
        }

        private QueryFragmentExecutable getFragment(int edgeId) {
            return inboxEdgeToFragment.get(edgeId);
        }

        private long getTimeout() {
            return timeout;
        }
    }
}
