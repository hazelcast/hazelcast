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

package com.hazelcast.sql.impl.mailbox.flowcontrol.simple;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.mailbox.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.HashMap;
import java.util.UUID;

/**
 * Simple implementation of a flow control that sends the flow control messages when certain memory threshold is reached.
 */
public class SimpleFlowControl implements FlowControl {
    /** Low watermark: denotes low memory condition. */
    // TODO: How we choose it? Should it be dynamic? Investigate exact flow control algorithms.
    private static final double LWM_PERCENTAGE = 0.4f;

    /** Maximum amount of memory allowed to be consumed by local stream. */
    // TODO: Now it is static. We may change dynamically to speedup or slowdown queries. But what heuristics to use?
    private final long maxMemory;

    /** Remote sources. */
    private HashMap<UUID, SimpleFlowControlStream> memberMap;

    /** Remote sources which should be notified. */
    private HashMap<UUID, SimpleFlowControlStream> pendingStreams;

    private QueryId queryId;
    private int edgeId;
    private QueryOperationHandler operationHandler;

    public SimpleFlowControl(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    @Override
    public void setup(QueryId queryId, int edgeId, QueryOperationHandler operationHandler) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.operationHandler = operationHandler;
    }

    @Override
    public void onBatchAdded(UUID memberId, long size, boolean last, long remoteMemory) {
        if (last) {
            // If this is the last batch, we do not care about backpressure.
            if (memberMap != null) {
                memberMap.remove(memberId);
            }

            if (pendingStreams != null) {
                pendingStreams.remove(memberId);
            }

            return;
        }

        // Otherwise save the current state.
        if (memberMap == null) {
            memberMap = new HashMap<>();

            memberMap.put(memberId, new SimpleFlowControlStream(memberId, remoteMemory, maxMemory - size));
        } else {
            SimpleFlowControlStream state = memberMap.get(memberId);

            if (state != null) {
                state.updateMemory(remoteMemory, state.getLocalMemory() - size);
            } else {
                memberMap.put(memberId, new SimpleFlowControlStream(memberId, remoteMemory, maxMemory - size));
            }
        }
    }

    @Override
    public void onBatchRemoved(UUID memberId, long size, boolean last) {
        // Micro-opt to avoid map lookup for the last batch and map instantiation.
        if (last) {
            return;
        }

        assert memberMap != null;

        SimpleFlowControlStream state = memberMap.get(memberId);

        if (state == null) {
            // Missing state means that last batch already arrived.
            return;
        }

        long remoteMemory = state.getRemoteMemory();
        long localMemory = state.getLocalMemory() + size;

        state.updateMemory(remoteMemory, localMemory);

        if (isLowMemory(remoteMemory) && !isLowMemory(localMemory)) {
            if (!state.isShouldSend()) {
                state.setShouldSend(true);

                if (pendingStreams == null) {
                    pendingStreams = new HashMap<>();
                }

                pendingStreams.put(memberId, state);
            }
        }
    }

    @Override
    public void onFragmentExecutionCompleted() {
        if (pendingStreams == null || pendingStreams.isEmpty()) {
            return;
        }

        for (SimpleFlowControlStream stream : pendingStreams.values()) {
            sendFlowControl(stream);

            stream.setShouldSend(false);
        }

        pendingStreams.clear();
    }

    /**
     * Send flow control message for the given stream.
     *
     * @param stream Stream.
     */
    private void sendFlowControl(SimpleFlowControlStream stream) {
        QueryFlowControlExchangeOperation operation = new QueryFlowControlExchangeOperation(
            queryId,
            edgeId,
            stream.getLocalMemory()
        );

        boolean success = operationHandler.submit(stream.getMemberId(), operation);

        if (!success) {
            throw HazelcastSqlException.error(SqlErrorCode.MEMBER_LEAVE,
                "Failed to send control flow message to member: " + stream.getMemberId());
        }
    }

    protected boolean isLowMemory(long availableMemory) {
        double percentage = ((double) availableMemory) / maxMemory;

        return percentage <= LWM_PERCENTAGE;
    }
}
