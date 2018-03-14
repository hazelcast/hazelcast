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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merge entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class QueueMergeOperation extends QueueBackupAwareOperation implements MutatingOperation {

    private SplitBrainMergePolicy mergePolicy;
    private List<MergingValue<Data>> mergingValues;

    private transient Map<Long, Data> valueMap;

    public QueueMergeOperation() {
    }

    public QueueMergeOperation(String name, SplitBrainMergePolicy mergePolicy,
                               List<MergingValue<Data>> mergingValues) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergingValues = mergingValues;
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        valueMap = createHashMap(mergingValues.size());
        for (MergingValue<Data> mergingValue : mergingValues) {
            QueueItem mergedItem = queueContainer.merge(mergingValue, mergePolicy);
            if (mergedItem != null) {
                valueMap.put(mergedItem.getItemId(), mergedItem.getData());
            }
        }
    }

    @Override
    public void afterRun() {
        getQueueService().getLocalQueueStatsImpl(name).incrementOtherOperations();
    }

    @Override
    public boolean shouldBackup() {
        return valueMap != null && !valueMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new AddAllBackupOperation(name, valueMap);
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.MERGE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeInt(mergingValues.size());
        for (MergingValue<Data> mergingValue : mergingValues) {
            out.writeObject(mergingValue);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        int size = in.readInt();
        mergingValues = new ArrayList<MergingValue<Data>>(size);
        for (int i = 0; i < size; i++) {
            MergingValue<Data> mergingValue = in.readObject();
            mergingValues.add(mergingValue);
        }
    }
}
