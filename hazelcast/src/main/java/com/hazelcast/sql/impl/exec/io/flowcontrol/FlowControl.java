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

package com.hazelcast.sql.impl.exec.io.flowcontrol;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.UUID;

/**
 * An interface to abstract out flow control implementations.
 * <p>
 * One instance of a {@code FlowControl} object is created per sender.
 * <p>
 * Internally the flow control object may act independently or rely on some shared state.
 * <p>
 * It is not specified when exactly the flow control message is sent back. It may happen during batch add/remove, or any other
 * event.
 */
public interface FlowControl {
    /**
     * One-time initialization routine.
     *
     * @param queryId ID of the owning query.
     * @param edgeId Edge ID.
     * @param localMemberId Local member ID that is presumed to be valid for the duration of query.
     * @param operationHandler Operation handler that will be used for operation.
     */
    void setup(QueryId queryId, int edgeId, UUID localMemberId, QueryOperationHandler operationHandler);

    /**
     * Callback invoked when a batch is added to the inbox.
     *
     * @param memberId Remote member ID.
     * @param size Size of the batch in bytes.
     * @param last Whether this is the last batch in the stream. Implementations should use this flag to avoid sending control
     *             flow messages back for closed streams.
     * @param remoteMemory The amount of remaining memory available to the sender by the time the batch was sent.
     */
    void onBatchAdded(UUID memberId, long size, boolean last, long remoteMemory);

    /**
     * Callback invoked when a previously added batch is consumed by the operator.
     *
     * @param memberId Remote member ID.
     * @param size Size of the batch in bytes.
     * @param last What this is the last batch in the stream.
     */
    void onBatchRemoved(UUID memberId, long size, boolean last);

    /**
     * Callback invoked when execution of a fragment is completed.
     */
    void onFragmentExecutionCompleted();
}
