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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook.CONTAINER;
import static com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook.F_ID;
import static com.hazelcast.util.MapUtil.createHashMap;

public class SemaphoreContainer implements IdentifiedDataSerializable {

    public static final int INITIAL_CAPACITY = 10;

    private int available;
    private int partitionId;
    private Map<String, Integer> attachMap;
    private int backupCount;
    private int asyncBackupCount;
    private boolean initialized;

    public SemaphoreContainer() {
    }

    public SemaphoreContainer(int partitionId, SemaphoreConfig config) {
        this.partitionId = partitionId;
        this.backupCount = config.getBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
        this.available = config.getInitialPermits();
        this.attachMap = new HashMap<String, Integer>(INITIAL_CAPACITY);
    }

    private void attach(String owner, int permitCount) {
        Integer attached = attachMap.get(owner);
        if (attached == null) {
            attached = 0;
        }
        attachMap.put(owner, attached + permitCount);
    }

    private void detach(String owner, int permitCount) {
        Integer attached = attachMap.get(owner);
        if (attached == null) {
            return;
        }

        attached -= permitCount;
        if (attached <= 0) {
            attachMap.remove(owner);
        } else {
            attachMap.put(owner, attached);
        }
    }

    public boolean detachAll(String owner) {
        Integer attached = attachMap.remove(owner);
        if (attached != null) {
            available += attached;
            return true;
        }
        return false;
    }

    public boolean init(int permitCount) {
        if (initialized || available != 0) {
            return false;
        }
        available = permitCount;
        initialized = true;
        return true;
    }

    public int getAvailable() {
        return available < 0 ? 0 : available;
    }

    public boolean isAvailable(int permitCount) {
        return available > 0 && available - permitCount >= 0;
    }

    public boolean acquire(String owner, int permitCount) {
        if (isAvailable(permitCount)) {
            available -= permitCount;
            attach(owner, permitCount);
            initialized = true;
            return true;
        }
        return false;
    }

    public int drain(String owner) {
        int drain = available;
        available = 0;
        if (drain > 0) {
            initialized = true;
            attach(owner, drain);
        }
        return drain;
    }

    public boolean increase(int permitCount) {
        if (permitCount == 0) {
            return false;
        }
        int newAvailable = available + permitCount;
        if (newAvailable < available) {
            return false;
        }
        available = newAvailable;
        return true;
    }

    public boolean reduce(int permitCount) {
        if (permitCount == 0) {
            return false;
        }
        int newAvailable = available - permitCount;
        if (newAvailable > available) {
            return false;
        }
        available = newAvailable;
        return true;
    }

    public void release(String owner, int permitCount) {
        available += permitCount;
        initialized = true;
        detach(owner, permitCount);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getSyncBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public void setInitialized() {
        this.initialized = true;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(available);
        out.writeInt(partitionId);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(attachMap.size());
        for (Map.Entry<String, Integer> entry : attachMap.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        available = in.readInt();
        partitionId = in.readInt();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        int size = in.readInt();
        attachMap = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String owner = in.readUTF();
            Integer val = in.readInt();
            attachMap.put(owner, val);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Permit");
        sb.append("{available=").append(available);
        sb.append(", partitionId=").append(partitionId);
        sb.append(", backupCount=").append(backupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append('}');
        sb.append("\n");
        for (Map.Entry<String, Integer> entry : attachMap.entrySet()) {
            sb.append("{owner=").append(entry.getKey());
            sb.append(", attached=").append(entry.getValue());
            sb.append("} ");
        }
        return sb.toString();
    }
}
