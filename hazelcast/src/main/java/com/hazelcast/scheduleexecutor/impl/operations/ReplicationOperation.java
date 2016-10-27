/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduleexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduleexecutor.impl.BackupTaskDescriptor;
import com.hazelcast.scheduleexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorPartition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReplicationOperation
        extends AbstractSchedulerOperation {

    private Map<String, Map<String, BackupTaskDescriptor>> map;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, Map<String, BackupTaskDescriptor>> map) {
        this.map = map;
    }

    @Override
    public void run() throws Exception {
        DistributedScheduledExecutorService service = getService();
        ScheduledExecutorPartition partition = service.getPartition(getPartitionId());
        for (Map.Entry<String, Map<String, BackupTaskDescriptor>> entry : map.entrySet()) {
            partition.createContainer(entry.getKey(), entry.getValue());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, Map<String, BackupTaskDescriptor>> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Map.Entry<String, BackupTaskDescriptor> subEntry : entry.getValue().entrySet()) {
                out.writeUTF(subEntry.getKey());
                out.writeObject(subEntry.getValue());
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        map = new HashMap<String, Map<String, BackupTaskDescriptor>>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            int subSize = in.readInt();
            Map<String, BackupTaskDescriptor> subMap = new HashMap<String, BackupTaskDescriptor>(subSize);
            map.put(key, subMap);
            for (int k = 0; k < subSize; k++) {
                subMap.put(in.readUTF(), (BackupTaskDescriptor) in.readObject());
            }
        }
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.REPLICATION;
    }

}
