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
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorDataSerializerHook;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ReplicationOperation
        extends AbstractSchedulerOperation {

    private Map<String, List<BackupTaskDescriptor>> map;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, List<BackupTaskDescriptor>> map) {
        this.map = map;
    }

    @Override
    public void run() throws Exception {
//        DistributedScheduledExecutorService service = getService();
//        ScheduledExecutorPartition partition = service.getPartition(getPartitionId());
//        for (Map.Entry<String, List<BackupTaskDescriptor>> entry : map.entrySet()) {
//            partition.createContainer(entry.getKey(), entry.getValue());
//        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
//        out.writeInt(map.size());
//        for (Map.Entry<String, ScheduledExecutorContainer> entry : map.entrySet()) {
//            out.writeUTF(entry.getKey());
//            out.writeObject(entry.getValue());
//        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
//        int size = in.readInt();
//        map = new HashMap<String, ScheduledExecutorContainer>(size);
//        for (int i = 0; i < size; i++) {
//            map.put(in.readUTF(), (ScheduledExecutorContainer) in.readObject());
//        }
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
