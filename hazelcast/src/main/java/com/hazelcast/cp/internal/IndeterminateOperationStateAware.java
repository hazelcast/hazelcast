/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.IndeterminateOperationStateException;

/**
 * When a {@link RaftOp} is replicated to the leader of a Raft group,
 * but the leader fails before it sends any response, the caller has no idea
 * about the status of its operation. The possible cases are:
 * - the failed leader did not append the operation,
 * - the failed leader appended the operation but not committed yet,
 * - the failed leader committed the operation.
 * <p>
 * In this case, we check if the operation is safe to retry. The operation is
 * considered to be safe to retry only if it implements this interface and
 * {@link #isRetryableOnIndeterminateOperationState()} method returns
 * {@code true}. If this is the case, the operation will be retried. Otherwise,
 * we do not know if the operation is safe or not. In this case, we make a
 * decision between offering at-most-once and at-least-once execution
 * semantics via the
 * {@link CPSubsystemConfig#failOnIndeterminateOperationState} configuration.
 * If this configuration is enabled, we do not retry the operation and fail
 * with {@link IndeterminateOperationStateException}, hence offer at-most-once
 * semantics. Otherwise, we retry the operation and we are fine with a possible
 * duplicate commit of the operation.
 */
public interface IndeterminateOperationStateAware {

    /**
     * Returns true if duplicate commit of the operation is equivalent to
     * committing it only once.
     */
    boolean isRetryableOnIndeterminateOperationState();

}
