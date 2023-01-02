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

package com.hazelcast.map.impl.proxy;

/**
 * Specifies action takes when {@link
 * com.hazelcast.spi.properties.ClusterProperty#OPERATION_BACKUP_TIMEOUT_MILLIS}
 * elapses when waiting for completion of synchronous backups.
 */
public enum SyncBackupTimeoutPolicy {
    /**
     * Use behavior configured by global property:
     * {@link com.hazelcast.spi.properties.ClusterProperty#FAIL_ON_INDETERMINATE_OPERATION_STATE} or
     * {@link com.hazelcast.client.properties.ClientProperty#FAIL_ON_INDETERMINATE_OPERATION_STATE}
     */
    GLOBAL(0),
    /**
     * In case of timeout for sync backup return success
     */
    SUCCESS(1),
    /**
     * In case of timeout for sync backup throw {@link com.hazelcast.core.IndeterminateOperationStateException}
     */
    INDETERMINATE(2);

    private final int id;

    SyncBackupTimeoutPolicy(int id) {
        this.id = id;
    }

    /**
     * Gets the ID for the given {@link SyncBackupTimeoutPolicy}.
     *
     * @return the ID
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the {@link SyncBackupTimeoutPolicy} as an enum.
     *
     * @return the {@link SyncBackupTimeoutPolicy} as an enum
     */
    public static SyncBackupTimeoutPolicy getById(final int id) {
        for (SyncBackupTimeoutPolicy type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
