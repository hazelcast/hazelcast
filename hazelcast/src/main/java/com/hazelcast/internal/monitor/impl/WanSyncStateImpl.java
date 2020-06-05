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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.wan.impl.WanSyncStatus;

@SuppressWarnings("unused")
public class WanSyncStateImpl implements WanSyncState {

    private long creationTime;
    private WanSyncStatus status = WanSyncStatus.READY;
    private int syncedPartitionCount;
    private String activeWanConfigName;
    private String activePublisherName;

    public WanSyncStateImpl() { }

    public WanSyncStateImpl(WanSyncStatus status, int syncedPartitionCount,
                            String activeWanConfigName, String activePublisherName) {
        creationTime = Clock.currentTimeMillis();
        this.status = status;
        this.syncedPartitionCount = syncedPartitionCount;
        this.activeWanConfigName = activeWanConfigName;
        this.activePublisherName = activePublisherName;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public WanSyncStatus getStatus() {
        return status;
    }

    @Override
    public int getSyncedPartitionCount() {
        return syncedPartitionCount;
    }

    @Override
    public String getActiveWanConfigName() {
        return activeWanConfigName;
    }

    @Override
    public String getActivePublisherName() {
        return activePublisherName;
    }

    @Override
    public String toString() {
        return "WanSyncStateImpl{wanSyncStatus=" + status
                + ", syncedPartitionCount=" + syncedPartitionCount
                + ", activeWanConfigName=" + activeWanConfigName
                + ", activePublisherName=" + activePublisherName
                + '}';
    }
}
