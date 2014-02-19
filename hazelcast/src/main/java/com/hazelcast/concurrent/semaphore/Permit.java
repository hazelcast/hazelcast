/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Permit implements DataSerializable {

    public static final int INITIAL_CAPACITY = 10;

    private int available;
    private int partitionId;
    private Map<String, Integer> attachMap;
    private int backupCount;
    private int asyncBackupCount;
    private boolean initialized;

    public Permit() {
    }

    public Permit(int partitionId, SemaphoreConfig config) {
        this.partitionId = partitionId;
        this.backupCount = config.getBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
        this.available = config.getInitialPermits();
        this.attachMap = new HashMap<String, Integer>(INITIAL_CAPACITY);
    }

    private void attach(String caller, int permitCount) {
        Integer attached = attachMap.get(caller);
        if (attached == null) {
            attached = 0;
        }
        attachMap.put(caller, attached + permitCount);
    }

    private void detach(String caller, int permitCount) {
        Integer attached = attachMap.get(caller);
        if (attached == null) {
            return;
        }

        attached -= permitCount;
        if (attached <= 0) {
            attachMap.remove(caller);
        } else {
            attachMap.put(caller, attached);
        }
    }

    public boolean memberRemoved(String caller) {
        Integer attached = attachMap.remove(caller);
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
        return available;
    }

    public boolean isAvailable(int permitCount) {
        return available - permitCount >= 0;
    }

    public boolean acquire(int permitCount, String caller) {
        if (isAvailable(permitCount)) {
            available -= permitCount;
            attach(caller, permitCount);
            initialized = true;
            return true;
        }
        return false;
    }

    public int drain(String caller) {
        int drain = available;
        available = 0;
        if (drain > 0) {
            initialized = true;
            attach(caller, drain);
        }
        return drain;
    }

    public boolean reduce(int permitCount) {
        if (available == 0 || permitCount == 0) {
            return false;
        }
        available -= permitCount;
        if (available < 0) {
            available = 0;
        }
        return true;
    }

    public void release(int permitCount, String caller) {
        available += permitCount;
        initialized = true;
        detach(caller, permitCount);
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
        attachMap = new HashMap<String, Integer>(size);
        for (int i = 0; i < size; i++) {
            String caller = in.readUTF();
            Integer val = in.readInt();
            attachMap.put(caller, val);
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
            sb.append("{caller=").append(entry.getKey());
            sb.append(", attached=").append(entry.getValue());
            sb.append("} ");
        }
        return sb.toString();
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }
}
