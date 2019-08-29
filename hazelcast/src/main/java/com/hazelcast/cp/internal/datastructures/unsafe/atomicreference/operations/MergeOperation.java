/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicReferenceMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingValue;

/**
 * Merges a {@link AtomicReferenceMergeTypes} for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AtomicReferenceBackupAwareOperation {

    private SplitBrainMergePolicy<Object, AtomicReferenceMergeTypes> mergePolicy;
    private Data mergingValue;

    private transient Data backupValue;

    public MergeOperation() {
    }

    public MergeOperation(String name, SplitBrainMergePolicy<Object, AtomicReferenceMergeTypes> mergePolicy, Data mergingValue) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergingValue = mergingValue;
    }

    @Override
    public void run() throws Exception {
        AtomicReferenceService service = getService();
        boolean isExistingContainer = service.containsReferenceContainer(name);
        AtomicReferenceContainer container = isExistingContainer ? getReferenceContainer() : null;
        Data oldValue = isExistingContainer ? container.get() : null;

        SerializationService serializationService = getNodeEngine().getSerializationService();
        serializationService.getManagedContext().initialize(mergePolicy);

        AtomicReferenceMergeTypes mergeValue = createMergingValue(serializationService, mergingValue);
        AtomicReferenceMergeTypes existingValue = isExistingContainer ? createMergingValue(serializationService, oldValue) : null;
        Data newValue = serializationService.toData(mergePolicy.merge(mergeValue, existingValue));

        backupValue = setNewValue(service, container, oldValue, newValue);
        shouldBackup = (backupValue == null && oldValue != null) || (backupValue != null && !backupValue.equals(oldValue));
    }

    private Data setNewValue(AtomicReferenceService service, AtomicReferenceContainer container, Data oldValue, Data newValue) {
        if (newValue == null) {
            service.destroyDistributedObject(name);
            return null;
        } else if (container == null) {
            container = getReferenceContainer();
            container.set(newValue);
            return newValue;
        } else if (!newValue.equals(oldValue)) {
            container.set(newValue);
            return newValue;
        }
        return oldValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(name, backupValue);
    }

    @Override
    public int getClassId() {
        return AtomicReferenceDataSerializerHook.MERGE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeData(mergingValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        mergingValue = in.readData();
    }
}
