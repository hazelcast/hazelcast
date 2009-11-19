/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

public enum ClusterOperation {
    NONE(),
    RESPONSE(),
    HEARTBEAT(),
    REMOTELY_PROCESS(),
    REMOTELY_PROCESS_AND_RESPOND(),
    REMOTELY_CALLABLE_BOOLEAN(),
    REMOTELY_CALLABLE_OBJECT(),
    ADD_LISTENER(),
    REMOVE_LISTENER(),
    EVENT(),
    REMOTELY_EXECUTE(),
    STREAM(),
    BLOCKING_QUEUE_POLL(),
    BLOCKING_QUEUE_OFFER(),
    BLOCKING_QUEUE_ADD_BLOCK(),
    BLOCKING_QUEUE_REMOVE_BLOCK(),
    BLOCKING_QUEUE_FULL_BLOCK(),
    BLOCKING_QUEUE_BACKUP_ADD(),
    BLOCKING_QUEUE_BACKUP_REMOVE(),
    BLOCKING_QUEUE_SIZE(),
    BLOCKING_QUEUE_PEEK(),
    BLOCKING_QUEUE_READ(),
    BLOCKING_QUEUE_REMOVE(),
    BLOCKING_QUEUE_TXN_BACKUP_POLL(),
    BLOCKING_QUEUE_TXN_COMMIT(),
    BLOCKING_QUEUE_PUBLISH(),
    BLOCKING_QUEUE_ADD_TOPIC_LISTENER(),
    CONCURRENT_MAP_PUT(),
    CONCURRENT_MAP_GET(),
    CONCURRENT_MAP_REMOVE(),
    CONCURRENT_MAP_REMOVE_ITEM(),
    CONCURRENT_MAP_GET_MAP_ENTRY(),
    CONCURRENT_MAP_BLOCK_INFO(),
    CONCURRENT_MAP_SIZE(),
    CONCURRENT_MAP_CONTAINS(),
    CONCURRENT_MAP_ITERATE_ENTRIES(),
    CONCURRENT_MAP_ITERATE_KEYS(),
    CONCURRENT_MAP_ITERATE_KEYS_ALL(),
    CONCURRENT_MAP_ITERATE_VALUES(),
    CONCURRENT_MAP_LOCK(),
    CONCURRENT_MAP_UNLOCK(),
    CONCURRENT_MAP_BLOCKS(),
    CONCURRENT_MAP_MIGRATION_COMPLETE(),
    CONCURRENT_MAP_CONTAINS_VALUE(),
    CONCURRENT_MAP_PUT_IF_ABSENT(),
    CONCURRENT_MAP_REMOVE_IF_SAME(),
    CONCURRENT_MAP_REPLACE_IF_NOT_NULL(),
    CONCURRENT_MAP_REPLACE_IF_SAME(),
    CONCURRENT_MAP_LOCK_RETURN_OLD(),
    CONCURRENT_MAP_READ(),
    CONCURRENT_MAP_ADD_TO_LIST(),
    CONCURRENT_MAP_ADD_TO_SET(),
    CONCURRENT_MAP_MIGRATE_RECORD(),
    CONCURRENT_MAP_PUT_MULTI(),
    CONCURRENT_MAP_REMOVE_MULTI(),
    CONCURRENT_MAP_VALUE_COUNT(),
    CONCURRENT_MAP_BACKUP_PUT(),
    CONCURRENT_MAP_BACKUP_REMOVE(),
    CONCURRENT_MAP_BACKUP_REMOVE_MULTI(),
    CONCURRENT_MAP_BACKUP_LOCK(),
    CONCURRENT_MAP_BACKUP_ADD(),
    CONCURRENT_MAP_EVICT(),
    TRANSACTION_BEGIN(),
    TRANSACTION_COMMIT(),
    TRANSACTION_ROLLBACK(),
    DESTROY(),
    GET_ID(),
    NEW_ID(),
    ADD_INDEX(),;

    static int idGen = 0;
    public final static int OPERATION_COUNT = 100;
    static ClusterOperation[] clusterOperations = new ClusterOperation[OPERATION_COUNT];

    static {
        ClusterOperation[] cops = ClusterOperation.values();
        for (ClusterOperation cop : cops) {
            clusterOperations[cop.getValue()] = cop;
        }
    }

    private int value;

    ClusterOperation() {
        value = nextId();
    }

    public int getValue() {
        return value;
    }

    private int nextId() {
        return idGen++;
    }

    public static ClusterOperation create(int operation) {
        return clusterOperations[operation];
    }
}
