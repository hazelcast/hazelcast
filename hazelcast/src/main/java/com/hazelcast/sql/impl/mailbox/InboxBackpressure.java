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

package com.hazelcast.sql.impl.mailbox;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

/**
 * Controls backpressure for a set of remote sources.
 */
public class InboxBackpressure {
    /** Low watermark: denotes low memory condition. */
    // TODO: How we choose it? Should it be dynamic? Investigate exact flow control algorithms.
    private static final double LWM_PERCENTAGE = 0.4f;

    /** Maximum amount of memory allowed to be consumed by local stream. */
    // TODO: Now it is static. We may change dynamically to speedup or slowdown queries. But what heuristics to use?
    private final long maxMemory;

    /** Remote sources. */
    private HashMap<UUID, InboxBackpressureState> memberMap;

    /** Remote sources which should be notified. */
    private HashMap<UUID, InboxBackpressureState> pendingMemberMap;

    public InboxBackpressure(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public void onBatchAdded(UUID memberId, boolean last, long remoteMemory, long localMemoryDelta) {
        if (last) {
            // If this is the last batch, we do not care about backpressure.
            if (memberMap != null) {
                memberMap.remove(memberId);
            }

            if (pendingMemberMap != null) {
                pendingMemberMap.remove(memberId);
            }

            return;
        }

        // Otherwise save the current state.
        if (memberMap == null) {
            memberMap = new HashMap<>();

            memberMap.put(memberId, new InboxBackpressureState(memberId, remoteMemory, maxMemory - localMemoryDelta));
        } else {
            InboxBackpressureState state = memberMap.get(memberId);

            if (state != null) {
                state.updateMemory(remoteMemory, state.getLocalMemory() - localMemoryDelta);
            } else {
                memberMap.put(memberId, new InboxBackpressureState(memberId, remoteMemory, maxMemory - localMemoryDelta));
            }
        }
    }

    public void onBatchRemoved(UUID memberId, boolean last, long localMemoryDelta) {
        // Micro-opt to avoid map lookup for the last batch and map instantiation.
        if (last) {
            return;
        }

        assert memberMap != null;

        InboxBackpressureState state = memberMap.get(memberId);

        if (state == null) {
            // Missing state means that last batch already arrived.
            return;
        }

        long remoteMemory = state.getRemoteMemory();
        long localMemory = state.getLocalMemory() + localMemoryDelta;

        state.updateMemory(remoteMemory, localMemory);

        if (isLowMemory(remoteMemory) && !isLowMemory(localMemory)) {
            if (!state.isShouldSend()) {
                state.setShouldSend(true);

                if (pendingMemberMap == null) {
                    pendingMemberMap = new HashMap<>();
                }

                pendingMemberMap.put(memberId, state);
            }
        }
    }

    public Collection<InboxBackpressureState> getPending() {
        return pendingMemberMap != null && !pendingMemberMap.isEmpty() ? pendingMemberMap.values() : Collections.emptySet();
    }

    public void clearPending() {
        if (pendingMemberMap != null) {
            pendingMemberMap.clear();
        }
    }

    protected boolean isLowMemory(long targetMemory) {
        double percentage = ((double) targetMemory) / maxMemory;

        return percentage <= LWM_PERCENTAGE;
    }
}
