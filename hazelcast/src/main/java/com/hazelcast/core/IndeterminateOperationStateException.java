/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.spi.properties.GroupProperty;

/**
 * IndeterminateOperationStateException is thrown when an invocation doesn't receive enough ACKs from the backup replicas in time.
 * This timeout is defined by configuration property {@link GroupProperty#OPERATION_BACKUP_TIMEOUT_MILLIS}.
 * <p>
 * Similarly, if the member, which owns the primary replica of the operation's target partition, leaves the cluster
 * before a response is returned, then operation is not retried but fails with IndeterminateOperationStateException.
 * However, there will not be any rollback on other successful replicas.
 * <p>
 * IndeterminateOperationStateException only informs the caller that the operation may not be executed on all requested backup
 * replicas, hence durability of the written / updated value may not be guaranteed immediately.
 *
 * @see GroupProperty#OPERATION_BACKUP_TIMEOUT_MILLIS
 * @see GroupProperty#FAIL_ON_INDETERMINATE_OPERATION_STATE
 */
public class IndeterminateOperationStateException extends HazelcastException {

    public IndeterminateOperationStateException() {
    }

    public IndeterminateOperationStateException(String message) {
        super(message);
    }

    public IndeterminateOperationStateException(String message, Throwable cause) {
        super(message, cause);
    }

}
