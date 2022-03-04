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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorPartition;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class ReplicationOperation
        extends AbstractSchedulerOperation {

    private Map<String, Map<String, ScheduledTaskDescriptor>> map;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, Map<String, ScheduledTaskDescriptor>> map) {
        this.map = map;
    }

    @Override
    public void run()
            throws Exception {
        DistributedScheduledExecutorService service = getService();
        ScheduledExecutorPartition partition = service.getPartition(getPartitionId());
        for (Map.Entry<String, Map<String, ScheduledTaskDescriptor>> entry : map.entrySet()) {
            ScheduledExecutorContainer container = partition.getOrCreateContainer(entry.getKey());
            for (Map.Entry<String, ScheduledTaskDescriptor> descriptorEntry : entry.getValue().entrySet()) {
                String taskName = descriptorEntry.getKey();
                ScheduledTaskDescriptor descriptor = descriptorEntry.getValue();

                if (!container.has(taskName)) {
                    container.enqueueSuspended(descriptor, false);
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, Map<String, ScheduledTaskDescriptor>> entry : map.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Map.Entry<String, ScheduledTaskDescriptor> subEntry : entry.getValue().entrySet()) {
                out.writeString(subEntry.getKey());
                out.writeObject(subEntry.getValue());
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        int size = in.readInt();
        map = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int subSize = in.readInt();
            Map<String, ScheduledTaskDescriptor> subMap = createHashMap(subSize);
            map.put(key, subMap);
            for (int k = 0; k < subSize; k++) {
                subMap.put(in.readString(), (ScheduledTaskDescriptor) in.readObject());
            }
        }
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.REPLICATION;
    }

}
