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

package com.hazelcast.sql.impl.exec.io.flowcontrol.simple;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.HashMap;
import java.util.UUID;

/**
 * Simple implementation of a flow control. The flow control message is sent when the remote end thinks that local end is low
 * on memory.
 */
public class SimpleFlowControl implements FlowControl {
    /** Default threashold. */
    static final double THRESHOLD_PERCENTAGE = 0.25f;

    /** Maximum amount of memory allowed to be consumed by the local stream. */
    private final long maxMemory;

    /** Low memory threshold in percents. */
    private final double thresholdPercentage;

    private QueryId queryId;
    private int edgeId;
    private UUID localMemberId;
    private QueryOperationHandler operationHandler;

    /** Remote streams. */
    private HashMap<UUID, SimpleFlowControlStream> streams;

    /** Remote streams that should be notified. */
    private HashMap<UUID, SimpleFlowControlStream> pendingStreams;

    public SimpleFlowControl(long maxMemory, double thresholdPercentage) {
        this.maxMemory = maxMemory;
        this.thresholdPercentage = thresholdPercentage;
    }

    @Override
    public void setup(QueryId queryId, int edgeId, UUID localMemberId, QueryOperationHandler operationHandler) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.localMemberId = localMemberId;
        this.operationHandler = operationHandler;
    }

    @Override
    public void onBatchAdded(UUID memberId, long size, boolean last, long remoteMemory) {
        if (last) {
            // If this is the last batch, we do not care about backpressure.
            if (streams != null) {
                streams.remove(memberId);
            }

            if (pendingStreams != null) {
                pendingStreams.remove(memberId);
            }

            return;
        }

        // Otherwise save the current state.
        if (streams == null) {
            streams = new HashMap<>();

            streams.put(memberId, new SimpleFlowControlStream(memberId, remoteMemory, maxMemory - size));
        } else {
            SimpleFlowControlStream state = streams.get(memberId);

            if (state != null) {
                state.updateMemory(remoteMemory, state.getLocalMemory() - size);
            } else {
                streams.put(memberId, new SimpleFlowControlStream(memberId, remoteMemory, maxMemory - size));
            }
        }
    }

    @Override
    public void onBatchRemoved(UUID memberId, long size, boolean last) {
        // Micro-opt to avoid map lookup for the last batch and map instantiation.
        if (last) {
            return;
        }

        assert streams != null;

        SimpleFlowControlStream state = streams.get(memberId);

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

    public long getMaxMemory() {
        return maxMemory;
    }

    public double getThresholdPercentage() {
        return thresholdPercentage;
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

        boolean success = operationHandler.submit(localMemberId, stream.getMemberId(), operation);

        if (!success) {
            throw QueryException.memberConnection(stream.getMemberId());
        }
    }

    /**
     * Check whether the given amount of memory is below the watermark.
     *
     * @param availableMemory Available memory.
     * @return {@code true} if below the watermark.
     */
    private boolean isLowMemory(long availableMemory) {
        return ((double) availableMemory) / maxMemory <= thresholdPercentage;
    }
}
