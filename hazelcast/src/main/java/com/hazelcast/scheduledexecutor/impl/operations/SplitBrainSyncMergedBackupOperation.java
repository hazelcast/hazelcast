/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook.SPLIT_BRAIN_SYNC_MERGED_OP;

public class SplitBrainSyncMergedBackupOperation
        extends AbstractSchedulerOperation {

    private Set<ScheduledTaskDescriptor> descriptors;

    public SplitBrainSyncMergedBackupOperation() {
    }

    SplitBrainSyncMergedBackupOperation(String name, Set<ScheduledTaskDescriptor> descriptors) {
        super(name);
        this.descriptors = descriptors;
    }

    @Override
    public void run()
            throws Exception {
        ScheduledExecutorContainer container = getContainer();

        for (ScheduledTaskDescriptor descriptor : descriptors) {
            container.enqueueSuspended(descriptor, true);
        }
    }

    @Override
    public int getId() {
        return SPLIT_BRAIN_SYNC_MERGED_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(descriptors.size());
        for (ScheduledTaskDescriptor descriptor : descriptors) {
            out.writeObject(descriptor);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        descriptors = new HashSet<ScheduledTaskDescriptor>(size);
        for (int i = 0; i < size; i++) {
            ScheduledTaskDescriptor descriptor = in.readObject();
            descriptors.add(descriptor);
        }
    }
}
