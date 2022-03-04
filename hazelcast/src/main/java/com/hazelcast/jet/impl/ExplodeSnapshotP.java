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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.impl.execution.BroadcastEntry;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataKey;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataValueTerminator;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class ExplodeSnapshotP extends AbstractProcessor {

    private final Map<String, FlatMapper<byte[], Object>> vertexToFlatMapper = new HashMap<>();
    private final long expectedSnapshotId;
    private InternalSerializationService serializationService;

    ExplodeSnapshotP(Map<String, Integer> vertexToOrdinal, long expectedSnapshotId) {
        this.expectedSnapshotId = expectedSnapshotId;
        for (Entry<String, Integer> en : vertexToOrdinal.entrySet()) {
            Object oldValue = vertexToFlatMapper.put(en.getKey(), flatMapper(en.getValue(), this::traverser));
            assert oldValue == null : "Duplicate ordinal: " + en.getValue();
        }
    }

    @Override
    protected void init(@Nonnull Context context) {
        serializationService = ((ProcCtx) context).serializationService();
    }

    /* We can't close the BufferObjectDataInput cleanly. We close it when the returned traverser is fully iterated,
    but the caller might not fully iterate it and we have no opportunity to close it.
    On the other hand, the returned object doesn't hold any resources, so relying on the GC is sufficient.
    See #19799 */
    @SuppressWarnings("squid:S2095")
    private Traverser<Object> traverser(byte[] data) {
        BufferObjectDataInput in = serializationService.createObjectDataInput(data);

        return () -> uncheckCall(() -> {
            Object key = in.readObject();
            if (key == SnapshotDataValueTerminator.INSTANCE) {
                return null;
            }
            Object value = in.readObject();
            return key instanceof BroadcastKey
                    ? new BroadcastEntry(key, value)
                    : entry(key, value);
        });
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        if (((Entry) item).getKey() instanceof SnapshotValidationRecord.SnapshotValidationKey) {
            // ignore the validation record
            return true;
        }
        Entry<SnapshotDataKey, byte[]> castItem = (Entry<SnapshotDataKey, byte[]>) item;
        String vertexName = castItem.getKey().vertexName();
        FlatMapper<byte[], Object> flatMapper = vertexToFlatMapper.get(vertexName);
        if (flatMapper == null) {
            if (!vertexToFlatMapper.containsKey(vertexName)) {
                // log only once
                vertexToFlatMapper.put(vertexName, null);
                getLogger().warning("Data for unknown vertex found in the snapshot, ignoring. Vertex=" + vertexName);
            }
            return true;
        }
        long snapshotId = castItem.getKey().snapshotId();
        if (snapshotId != expectedSnapshotId) {
            getLogger().warning("Data for unexpected snapshot ID encountered, ignoring. Expected="
                    + expectedSnapshotId + ", found=" + snapshotId);
            return true;
        }
        return flatMapper.tryProcess(castItem.getValue());
    }
}
