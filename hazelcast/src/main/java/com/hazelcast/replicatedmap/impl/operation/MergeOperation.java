/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy;

import java.io.IOException;

/**
 * Merges two replicated map entries with the given merge policy after the split-brain syndrome is recovered.
 */
public class MergeOperation extends AbstractReplicatedMapOperation {

    private String name;
    private Object key;
    private ReplicatedMapEntryView entryView;
    private ReplicatedMapMergePolicy policy;

    public MergeOperation() {
    }

    public MergeOperation(String name, Object key, ReplicatedMapEntryView entryView, ReplicatedMapMergePolicy policy) {
        this.name = name;
        this.key = key;
        this.entryView = entryView;
        this.policy = policy;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, key);
        store.merge(key, entryView, policy);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        IOUtil.writeObject(out, key);
        out.writeObject(entryView);
        out.writeObject(policy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        key = IOUtil.readObject(in);
        entryView = in.readObject();
        policy = in.readObject();
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.MERGE;
    }
}
