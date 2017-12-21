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
