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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapMergeContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merge entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AbstractMultiMapOperation implements BackupAwareOperation {

    private List<MultiMapMergeContainer> mergeContainers;
    private SplitBrainMergePolicy<Object, MultiMapMergeTypes> mergePolicy;

    private transient Map<Data, Collection<MultiMapRecord>> resultMap;

    public MergeOperation() {
    }

    public MergeOperation(String name, List<MultiMapMergeContainer> mergeContainers,
                          SplitBrainMergePolicy<Object, MultiMapMergeTypes> mergePolicy) {
        super(name);
        this.mergeContainers = mergeContainers;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        resultMap = createHashMap(mergeContainers.size());
        for (MultiMapMergeContainer mergeContainer : mergeContainers) {
            Data key = mergeContainer.getKey();
            if (!container.canAcquireLock(key, getCallerUuid(), -1)) {
                Object valueKey = getNodeEngine().getSerializationService().toObject(key);
                getLogger().info("Skipped merging of locked key '" + valueKey + "' on MultiMap '" + name + "'");
                continue;
            }

            MultiMapValue result = container.merge(key, mergeContainer, mergePolicy);
            if (result != null) {
                resultMap.put(key, result.getCollection(false));
                publishEvent(EntryEventType.MERGED, key, result, null);
            }
        }
        response = !resultMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(name, resultMap);
    }

    @Override
    public boolean shouldBackup() {
        return !resultMap.isEmpty();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergeContainers.size());
        for (MultiMapMergeContainer container : mergeContainers) {
            out.writeObject(container);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergeContainers = new ArrayList<MultiMapMergeContainer>(size);
        for (int i = 0; i < size; i++) {
            MultiMapMergeContainer container = in.readObject();
            mergeContainers.add(container);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.MERGE_OPERATION;
    }
}
