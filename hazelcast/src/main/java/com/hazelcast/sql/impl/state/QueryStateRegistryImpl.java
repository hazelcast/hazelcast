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

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.SqlServiceImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Query state implementation.
 */
public class QueryStateRegistryImpl implements QueryStateRegistry {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** SQL service. */
    private final SqlServiceImpl service;

    /** IDs of locally started queries. */
    private final ConcurrentHashMap<QueryId, QueryState> states = new ConcurrentHashMap<>();

    /** Lock to prevent invalid concurrent access. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Local member ID. */
    private final UUID localMemberId;

    /** Current epoch. */
    private volatile long epoch;

    /** Epoch low watermark. */
    private volatile long epochLowWatermark;

    /** Collected watermarks of remote nodes. */
    private volatile HashMap<UUID, WatermarkHolder> memberEpochLowWatermarks = new HashMap<>();

    /** Mutex to update remote node watermarks. */
    private final Object memberEpochLowWatermarkMux = new Object();

    public QueryStateRegistryImpl(NodeEngine nodeEngine, SqlServiceImpl service, UUID localMemberId) {
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.localMemberId = localMemberId;
    }

    @Override
    public QueryState onInitiatorQueryStarted(
        long initiatorTimeout,
        QueryPlan initiatorPlan,
        IdentityHashMap<QueryFragment, Collection<UUID>> initiatorFragmentMappings,
        QueryStateRowSource initiatorRowSource,
        boolean isExplain
    ) {
        // Special handling for EXPLAIN: return the state without registering it.
        if (isExplain) {
            return QueryState.createInitiatorState(
                QueryId.create(localMemberId, epoch),
                localMemberId,
                null,
                initiatorTimeout,
                initiatorPlan,
                null,
                initiatorRowSource
            );
        }

        QueryStateCompletionCallback completionCallback = new QueryStateCompletionCallback(service, this);

        // Optimistic path: if the epoch has not changed during query registration, then the updater thread was either idle,
        // or it was active, but we read already updated epoch, so it is safe to register the query with that epoch.
        long epoch0 = epoch;

        QueryId queryId = QueryId.create(localMemberId, epoch0);

        QueryState state = QueryState.createInitiatorState(
            queryId,
            localMemberId,
            completionCallback,
            initiatorTimeout,
            initiatorPlan,
            initiatorFragmentMappings,
            initiatorRowSource
        );

        states.put(queryId, state);

        if (epoch0 == epoch) {
            return state;
        } else {
            // Epoch was changed concurrently, it is not safe to proceed with the given query ID.
            states.remove(queryId);
        }

        // Pessimistic path: get current epoch under the read lock.
        lock.readLock().lock();

        try {
            queryId = QueryId.create(localMemberId, epoch);

            state = QueryState.createInitiatorState(
                queryId,
                localMemberId,
                completionCallback,
                initiatorTimeout,
                initiatorPlan,
                initiatorFragmentMappings,
                initiatorRowSource
            );

            states.put(queryId, state);

            return state;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public QueryState onDistributedQueryStarted(QueryId queryId) {
        UUID initiatorMemberId =  queryId.getMemberId();

        boolean local = localMemberId.equals(initiatorMemberId);

        if (local) {
            // For locally initiated query, the state must already exist. If not - the query has been completed.
            return states.get(queryId);
        } else {
            // For non-locally initiated query we use epoch LWM to check whether the received request outdated or not.
            // If yes, new state is not created. Otherwise, initialize the distirbuted state.
            WatermarkHolder watermarkHolder = memberEpochLowWatermarks.get(initiatorMemberId);

            if (watermarkHolder != null && queryId.getEpoch() < watermarkHolder.get()) {
                return null;
            }

            QueryState state = states.get(queryId);

            if (state == null) {
                state = QueryState.createDistributedState(
                    queryId,
                    localMemberId,
                    new QueryStateCompletionCallback(service, this)
                );

                QueryState oldState = states.putIfAbsent(queryId, state);

                if (oldState != null) {
                    state = oldState;
                }
            }

            return state;
        }
    }

    @Override
    public void onQueryFinished(QueryId queryId) {
        states.remove(queryId);
    }

    @Override
    public QueryState getState(QueryId queryId) {
        return states.get(queryId);
    }

    @Override
    public boolean isLocalQueryActive(QueryId queryId) {
        return states.containsKey(queryId);
    }

    @Override
    public long getEpoch() {
        return epoch;
    }

    @Override
    public long getEpochLowWatermark() {
        return epochLowWatermark;
    }

    @Override
    public void setEpochLowWatermarkForMember(UUID memberId, long epochLowWatermark) {
        WatermarkHolder holder = memberEpochLowWatermarks.get(memberId);

        if (holder == null) {
            synchronized (memberEpochLowWatermarkMux) {
                holder = memberEpochLowWatermarks.get(memberId);

                if (holder == null) {
                    holder = new WatermarkHolder();

                    HashMap<UUID, WatermarkHolder> memberEpochWatermarks0 = new HashMap<>(memberEpochLowWatermarks);

                    memberEpochWatermarks0.put(memberId, holder);

                    memberEpochLowWatermarks = memberEpochWatermarks0;
                }
            }
        }

        holder.set(epochLowWatermark);
    }

    @Override
    public void update() {
        // Update the epoch.
        updateEpoch();

        // Prepare the list of members.
        // TODO: Looks like this is prety cheap operation, isn't it? If no, let's use membership listeners instead.
        Set<UUID> memberIds = new HashSet<>();

        for (Member member : nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR)) {
            memberIds.add(member.getUuid());
        }

        Map<UUID, Collection<QueryId>> checkMap = new HashMap<>();

        for (QueryState state : states.values()) {
            // 1. Check if the query has timed out.
            if (state.tryCancelOnTimeout()) {
                continue;
            }

            // 2. Check whether the member required for the query has left.
            if (state.tryCancelOnMemberLeave(memberIds)) {
                continue;
            }

            // 3. Check whether the query is not initialized for too long. If yes, trigger check process.
            if (state.requestQueryCheck()) {
                QueryId queryId = state.getQueryId();

                checkMap.computeIfAbsent(queryId.getMemberId(), (key) -> new ArrayList<>(1)).add(queryId);
            }
        }

        // Send batched check requests.
        for (Map.Entry<UUID, Collection<QueryId>> checkEntry : checkMap.entrySet()) {
            service.checkQuery(checkEntry.getKey(), checkEntry.getValue());
        }

        // Clear watermarks for left members
        clearOrphanWatermarks(memberIds);
    }

    private void clearOrphanWatermarks(Set<UUID> memberIds) {
        Set<UUID> orphanWatermarkMemberIds = new HashSet<>();

        for (UUID memberId : memberEpochLowWatermarks.keySet()) {
            if (!memberIds.contains(memberId)) {
                orphanWatermarkMemberIds.add(memberId);
            }
        }

        if (!orphanWatermarkMemberIds.isEmpty()) {
            synchronized (memberEpochLowWatermarkMux) {
                HashMap<UUID, WatermarkHolder> memberEpochWatermarks0 = new HashMap<>(memberEpochLowWatermarks);

                for (UUID orphanWatermarkMemberId : orphanWatermarkMemberIds) {
                    memberEpochWatermarks0.remove(orphanWatermarkMemberId);
                }

                memberEpochLowWatermarks = memberEpochWatermarks0;
            }
        }
    }

    private void updateEpoch() {
        lock.writeLock().lock();

        try {
            // Advance the epoch.
            long newEpoch = epoch + 1;

            epoch = newEpoch;

            // Find the query with the lowest epoch.
            long newEpochLowWatermark = Long.MAX_VALUE;

            for (QueryId localQueryId : states.keySet()) {
                long queryEpoch = localQueryId.getEpoch();

                if (queryEpoch < newEpochLowWatermark) {
                    newEpochLowWatermark = queryEpoch;
                }
            }

            // Make sure that the new LWM is not greater than the new epoch, e.g. if there are no active queries.
            if (newEpochLowWatermark > newEpoch) {
                newEpochLowWatermark = newEpoch;
            }

            // Finally, advance LWM.
            if (newEpochLowWatermark > epochLowWatermark) {
                epochLowWatermark = newEpochLowWatermark;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Holder for remote node watermark.
     */
    private static class WatermarkHolder {
        private volatile long watermark;

        private void set(long watermark) {
            this.watermark = watermark;
        }

        private long get() {
            return watermark;
        }
    }
}
