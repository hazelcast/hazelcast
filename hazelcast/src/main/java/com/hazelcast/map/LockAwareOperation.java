/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.Data;

import java.util.concurrent.TimeoutException;

public abstract class LockAwareOperation extends BackupAwareOperation {

    protected LockAwareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    protected LockAwareOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl);
    }

    protected LockAwareOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey, dataValue, ttl);
    }

    protected LockAwareOperation() {
    }

    public void run() {
        MapService mapService = (MapService) getService();
        int partitionId = getPartitionId();
        PartitionContainer pc = mapService.getPartitionContainer(partitionId);
        MapPartition mapPartition = pc.getMapPartition(name);
        if (!mapPartition.canRun(this)) {
            if (getTimeout() > 0) {
                pc.scheduleOp(this);
            } else {
                onNoTimeToSchedule();
            }
            return;
        }
        doOp();
    }

    abstract void doOp();

    protected void onNoTimeToSchedule() {
        onExpire();
    }

    protected void onExpire() {
        if (getTimeout() == 0) {
            getResponseHandler().sendResponse(Boolean.FALSE);
        } else {
            getResponseHandler().sendResponse(new TimeoutException());
        }
    }
}
