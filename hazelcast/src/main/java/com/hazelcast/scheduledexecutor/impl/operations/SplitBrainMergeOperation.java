package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook.SPLIT_BRAIN_MERGE_OP;

public class SplitBrainMergeOperation extends AbstractBackupAwareSchedulerOperation {

    private SplitBrainMergePolicy policy;

    private List<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>> mergingEntries;

    private transient Set<ScheduledTaskDescriptor> mergedTasks;

    public SplitBrainMergeOperation() {
        super();
    }

    public SplitBrainMergeOperation(String name, SplitBrainMergePolicy mergePolicy,
                                    List<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>> mergingEntries) {
        super(name);
        this.policy = mergePolicy;
        this.mergingEntries = mergingEntries;
    }

    @Override
    public boolean shouldBackup() {
        return super.shouldBackup() && mergedTasks != null && !mergedTasks.isEmpty();
    }

    @Override
    public void run()
            throws Exception {
        ScheduledExecutorContainer container = getContainer();
        mergedTasks = new HashSet<ScheduledTaskDescriptor>();

        for (SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> entry : mergingEntries) {
            ScheduledTaskDescriptor merged = container.merge(entry, policy);
            if (merged != null) {
                mergedTasks.add(merged);
            }
        }

        container.promoteSuspended();
    }

    @Override
    public int getId() {
        return SPLIT_BRAIN_MERGE_OP;
    }

    @Override
    public Operation getBackupOperation() {
        return new SplitBrainSyncMergedBackupOperation(getSchedulerName(), mergedTasks);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(policy);
        out.writeInt(mergingEntries.size());
        for (SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> entry : mergingEntries) {
            out.writeObject(entry);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        policy = in.readObject();
        int size = in.readInt();
        mergingEntries = new ArrayList<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>>(size);
        for (int i = 0; i < size; i++) {
            SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> entry = in.readObject();
            mergingEntries.add(entry);
        }
    }
}
