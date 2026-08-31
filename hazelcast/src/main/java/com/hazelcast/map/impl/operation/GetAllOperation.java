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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.steps.GetAllOpSteps;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class GetAllOperation extends MapOperation
        implements ReadonlyOperation, PartitionAwareOperation, Versioned {

    /**
     * Speculative factor to be used when initialising collections
     * of an approximate final size.
     */
    private static final double SIZING_FUDGE_FACTOR = 1.3;

    private List<Data> keys = new ArrayList<>();
    /**
     * If {@link #keys} contain keys only for current partition (new logic) or possibly many partitions (old logic)
     */
    private boolean singlePartition;
    private MapEntries entries;

    public GetAllOperation() {
    }

    public GetAllOperation(String name, List<Data> keys, boolean singlePartition) {
        super(name);
        this.keys = keys;
        this.singlePartition = singlePartition;
    }

    @Override
    protected void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        if (recordStore != null) {
            recordStore.checkIfLoaded();
        }
    }

    @Override
    protected void runInternal() {
        entries = recordStore.getAll(getPartitionKeySet(), getCallerAddress());
    }

    private Collection<Data> getPartitionKeySet() {
        if (singlePartition) {
            // Execution with MapStore will mutate provided key set to detect which entries should be loaded from MapStore.
            // In optimized member-side execution the list may be used also for sending to other members,
            // need to copy it to avoid concurrent modifications. Original set may be needed also for retries.
            return recordStore != null && hasMapStoreImplementation() ? new HashSet<>(keys) : keys;
        }
        // RUCOMPAT_5_7
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitionId = getPartitionId();
        final int roughSize = (int) (keys.size() * SIZING_FUDGE_FACTOR / partitionService.getPartitionCount());
        Set<Data> partitionKeySet = createHashSet(roughSize);
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        return partitionKeySet;
    }

    @Override
    public State createState() {
        return super.createState()
                .setKeys(getPartitionKeySet());
    }

    @Override
    public Step getStartingStep() {
        return GetAllOpSteps.READ;
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);
        entries = state.getMapEntries();
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
        // RUCOMPAT_5_7
        if (out.getVersion().isGreaterOrEqual(Versions.V6_0)) {
            out.writeBoolean(singlePartition);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = IOUtil.readData(in);
                keys.add(data);
            }
        }
        // RUCOMPAT_5_7
        if (in.getVersion().isGreaterOrEqual(Versions.V6_0)) {
            singlePartition = in.readBoolean();
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.GET_ALL;
    }
}
