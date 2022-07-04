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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CollectionMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;

/**
 * Merges a {@link CollectionMergeTypes} for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CollectionMergeOperation extends CollectionBackupAwareOperation {

    private SplitBrainMergePolicy<Collection<Object>, CollectionMergeTypes<Object>, Collection<Object>> mergePolicy;
    private CollectionMergeTypes mergingValue;

    private transient Collection<CollectionItem> backupCollection;
    private transient boolean shouldBackup;

    public CollectionMergeOperation(String name,
                                    SplitBrainMergePolicy<Collection<Object>, CollectionMergeTypes<Object>,
                                            Collection<Object>> mergePolicy, CollectionMergeTypes<Object> mergingValue) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergingValue = mergingValue;
    }

    public CollectionMergeOperation() {
    }

    @Override
    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        boolean currentCollectionIsEmpty = container.getCollection().isEmpty();
        long currentItemId = container.getCurrentId();

        backupCollection = merge(container, mergingValue, mergePolicy);
        shouldBackup = currentCollectionIsEmpty != backupCollection.isEmpty() || currentItemId != container.getCurrentId();
    }

    private Collection<CollectionItem> merge(CollectionContainer container, CollectionMergeTypes<Object> mergingValue,
                                             SplitBrainMergePolicy<Collection<Object>, CollectionMergeTypes<Object>,
                                                     Collection<Object>> mergePolicy) {
        SerializationService serializationService = getNodeEngine().getSerializationService();
        mergingValue = (CollectionMergeTypes<Object>) serializationService.getManagedContext().initialize(mergingValue);
        mergePolicy = (SplitBrainMergePolicy<Collection<Object>, CollectionMergeTypes<Object>, Collection<Object>>)
            serializationService.getManagedContext().initialize(mergePolicy);

        Collection<CollectionItem> existingItems = container.getCollection();

        CollectionMergeTypes<Object> existingValue = createMergingValueOrNull(serializationService, existingItems);
        Collection<Object> newValues = mergePolicy.merge(mergingValue, existingValue);

        if (isEmpty(newValues)) {
            RemoteService service = getService();
            service.destroyDistributedObject(name);
        } else if (existingValue == null) {
            createNewCollectionItems(container, existingItems, newValues, serializationService);
        } else if (!newValues.equals(existingValue.getRawValue())) {
            container.clear(false);
            createNewCollectionItems(container, existingItems, newValues, serializationService);
        }
        return existingItems;
    }

    private CollectionMergeTypes<Object> createMergingValueOrNull(SerializationService serializationService,
                                                                  Collection<CollectionItem> existingItems) {
        return existingItems.isEmpty() ? null : createMergingValue(serializationService, existingItems);
    }

    private void createNewCollectionItems(CollectionContainer container, Collection<CollectionItem> items,
                                          Collection<Object> values, SerializationService serializationService) {
        for (Object value : values) {
            CollectionItem item = new CollectionItem(container.nextId(), serializationService.toData(value));
            items.add(item);
        }
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionMergeBackupOperation(name, backupCollection);
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

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_MERGE;
    }
}
