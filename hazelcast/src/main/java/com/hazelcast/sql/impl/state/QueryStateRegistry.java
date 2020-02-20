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

import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryPlan;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.UUID;

/**
 * Query state manager.
 */
public interface QueryStateRegistry {
    /**
     * Callback invoked when the query is started on the initiator node.
     *
     * @return Query state.
     */
    QueryState onInitiatorQueryStarted(
        long initiatorTimeout,
        QueryPlan initatorPlan,
        IdentityHashMap<QueryFragment, Collection<UUID>> initiatorFragmentMappings,
        QueryStateRowSource initiatorRowSource,
        boolean isExplain
    );

    /**
     * Callback invoked when the query is started on participant.
     *
     * @return Query state.
     */
    QueryState onDistributedQueryStarted(QueryId queryId);

    /**
     * Callback invoked when the query is finished on the local node.
     *
     * @param queryId Query ID.
     */
    void onQueryFinished(QueryId queryId);

    /**
     * @param queryId Query ID.
     * @return Query state.
     */
    QueryState getState(QueryId queryId);

    /**
     * Check if the given local query is active.
     *
     * @param queryId Query ID.
     * @return {@code True} if active, {@code false} otherwise.
     */
    boolean isLocalQueryActive(QueryId queryId);

    /**
     * Get current epoch.
     *
     * @return Current epoch.
     */
    long getEpoch();

    /**
     * Get current epoch watermark.
     *
     * @return The lowest epoch of currently running queries.
     */
    long getEpochLowWatermark();

    /**
     * Set's epoch low watermark known for the given member.
     *
     * @param memberId Member ID.
     * @param epochLowWatermark Epoch low watermark.
     */
    void setEpochLowWatermarkForMember(UUID memberId, long epochLowWatermark);

    /**
     * Check query state and perform corrective actions if needed.
     */
    void update();
}
