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

package com.hazelcast.transaction.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Represents a change made in a transaction e.g. a map.put.
 *
 * @see Transaction
 */
public interface TransactionLogRecord extends IdentifiedDataSerializable {

    /**
     * Gets the transaction-log-key that uniquely identifies the {@link TransactionLogRecord} within the {@link TransactionLog}.
     *
     * E.g. for a map that would be the map-name and the key. So if a map transactional put is done on some
     * key, and if later on another put on that key is done, by using the same transaction-log-key, the first
     * put is overwritten.
     *
     * If null is returned, this TransactionLogRecord can't be identified and can't be overwritten by a later change.
     *
     * @return the transaction-log-key that uniquely identifies this TransactionLogRecord.
     */
    Object getKey();

    Operation newPrepareOperation();

    Operation newCommitOperation();

    default void onCommitSuccess() {
        // NOP
    }

    default void onCommitFailure() {
        // NOP
    }

    Operation newRollbackOperation();
}
