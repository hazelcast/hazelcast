/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.VectorCollectionMergeTypes;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.VectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.config.MergePolicyValidator.checkVectorCollectionMergePolicy;
import static com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage.MergeResponse.ENTRY_REMOVED;
import static com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage.MergeStatus.ENTRY_UPDATED;

public class MergeOperation extends BaseMutatingOperation
        implements PartitionAwareOperation, BackupAwareOperation {

    private List<VectorCollectionMergeTypes<Object, VectorDocument<?>>> mergingEntries;
    private SplitBrainMergePolicy<VectorDocument<?>,
            VectorCollectionMergeTypes<Object, VectorDocument<?>>, Object> mergePolicy;

    private transient VectorEntries updatedEntries;
    private transient List<Data> removedKeys;
    private transient boolean hasBackupsConfigured;
    private transient VectorCollectionConfig config;

    public MergeOperation() {
    }

    public MergeOperation(String name, List<VectorCollectionMergeTypes<Object, VectorDocument<?>>> mergingEntries,
                          SplitBrainMergePolicy<VectorDocument<?>,
                          VectorCollectionMergeTypes<Object, VectorDocument<?>>, Object> mergePolicy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void beforeRun() {
        super.beforeRun();

        config = storage.getConfig();
        checkVectorCollectionMergePolicy(config, config.getMergePolicyConfig().getPolicy(),
                getNodeEngine().getSplitBrainMergePolicyProvider());
        hasBackupsConfigured = config.getTotalBackupCount() > 0;
        updatedEntries = hasBackupsConfigured ? new VectorEntries() : null;
        removedKeys = hasBackupsConfigured ? new ArrayList<>(mergingEntries.size()) : null;
    }

    @Override
    public void run() throws Exception {
        // merge entries
        for (var entry : mergingEntries) {
            var mergeResponse = storage.merge(entry, mergePolicy);
            if (hasBackupsConfigured) {
                if (mergeResponse == ENTRY_REMOVED) {
                    removedKeys.add((Data) entry.getRawKey());
                } else if (mergeResponse.status() == ENTRY_UPDATED) {
                    var dataVectorDocument = VectorUtil.serialize(mergeResponse.mergedValue(),
                            getNodeEngine().getSerializationService());
                    updatedEntries.add((Data) entry.getRawKey(), dataVectorDocument);
                }
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return hasBackupsConfigured && (!updatedEntries.isEmpty() || !removedKeys.isEmpty());
    }

    @Override
    public int getSyncBackupCount() {
        return config.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return config.getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(name, updatedEntries, removedKeys);
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.MERGE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeList(mergingEntries, out);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntries = SerializationUtil.readList(in);
        mergePolicy = NamespaceUtil.callWithNamespace(in::readObject, name, VectorCollectionService::lookupNamespace);
    }
}
