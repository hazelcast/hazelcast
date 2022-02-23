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

package com.hazelcast.wan.impl;

/**
 * {@code WanSyncStatus} shows the current status of WAN sync.
 */
public enum WanSyncStatus {

    /**
     * Indicates existing sync operation, if exists, is completed
     * and/or member is available for new sync requests.
     */
    READY(0),

    /**
     * Indicates a sync operation is in progress on the member. At this state
     * member will not accept new sync requests.
     */
    IN_PROGRESS(1),

    /**
     * Indicates sync operation is unsuccessful.
     */
    FAILED(2);

    private int status;

    WanSyncStatus(final int status) {
        this.status = status;
    }

    /**
     * @return unique ID of the event type
     */
    public int getStatus() {
        return status;
    }

    public static WanSyncStatus getByStatus(final int status) {
        for (WanSyncStatus syncState : values()) {
            if (syncState.status == status) {
                return syncState;
            }
        }
        return null;
    }
}
