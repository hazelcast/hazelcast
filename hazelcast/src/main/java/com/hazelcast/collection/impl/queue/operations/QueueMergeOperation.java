/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.QueueMergeTypes;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;

/**
 * Merges a {@link QueueMergeTypes} for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class QueueMergeOperation extends QueueBackupAwareOperation implements MutatingOperation {

    private SplitBrainMergePolicy<Collection<QueueItem>, QueueMergeTypes<QueueItem>, Collection<QueueItem>> mergePolicy;
    private QueueMergeTypes<QueueItem> mergingValue;

    private transient Collection<QueueItem> backupCollection;
    private transient boolean shouldBackup;

    public QueueMergeOperation() {
    }

    public QueueMergeOperation(String name,
                               SplitBrainMergePolicy<Collection<QueueItem>, QueueMergeTypes<QueueItem>,
                                       Collection<QueueItem>> mergePolicy,
                               QueueMergeTypes<QueueItem> mergingValue) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergingValue = mergingValue;
    }

    @Override
    public void run() {
        QueueContainer container = getContainer();
        boolean currentCollectionIsEmpty = container.getItemQueue().isEmpty();
        long currentItemId = container.getCurrentId();

        backupCollection = merge(container, mergingValue, mergePolicy);
        shouldBackup = currentCollectionIsEmpty != backupCollection.isEmpty() || currentItemId != container.getCurrentId();
    }

    private Queue<QueueItem> merge(QueueContainer container, QueueMergeTypes<QueueItem> mergingValue,
                                   SplitBrainMergePolicy<Collection<QueueItem>, QueueMergeTypes<QueueItem>,
                                           Collection<QueueItem>> mergePolicy) {
        SerializationService serializationService = getNodeEngine().getSerializationService();
        mergingValue = (QueueMergeTypes<QueueItem>) serializationService.getManagedContext().initialize(mergingValue);
        mergePolicy = (SplitBrainMergePolicy<Collection<QueueItem>, QueueMergeTypes<QueueItem>, Collection<QueueItem>>)
            serializationService.getManagedContext().initialize(mergePolicy);

        Queue<QueueItem> existingItems = container.getItemQueue();
        QueueMergeTypes<QueueItem> existingValue = createMergingValueOrNull(serializationService, existingItems);
        mergePolicy.merge(mergingValue, existingValue);

//        if (isEmpty(newValues)) {
//            if (existingValue != null) {
//                container.clear();
//            }
//            getQueueService().destroyDistributedObject(name);
//        } else if (existingValue == null) {
//            createNewQueueItems(container, newValues, serializationService);
//        } else if (!newValues.equals(existingValue.getRawValue())) {
//            container.clear();
//            createNewQueueItems(container, newValues, serializationService);
//        }
        return existingItems;
    }

    private QueueMergeTypes<QueueItem> createMergingValueOrNull(SerializationService serializationService,
                                                             Queue<QueueItem> existingItems) {
        return createMergingValue(serializationService, existingItems);
    }

    private void createNewQueueItems(QueueContainer container, Collection<Object> values,
                                     SerializationService serializationService) {
        for (Object value : values) {
            container.offer(serializationService.toData(value));
        }
    }

    @Override
    public void afterRun() {
        getQueueService().getLocalQueueStatsImpl(name).incrementOtherOperations();
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new QueueMergeBackupOperation(name, backupCollection);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.MERGE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeObject(mergingValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        mergingValue = in.readObject();
    }
}
