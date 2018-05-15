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

package com.hazelcast.jet.impl;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.impl.execution.BroadcastEntry;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataValueTerminator;
import com.hazelcast.nio.BufferObjectDataInput;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class ExplodeSnapshotP extends AbstractProcessor {

    private FlatMapper<byte[], Object> flatMapper = flatMapper(this::traverser);
    private InternalSerializationService serializationService;

    @Override
    protected void init(@Nonnull Context context) {
        serializationService =
                ((HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance()).getSerializationService();
    }

    private Traverser<Object> traverser(byte[] data) {
        BufferObjectDataInput in = serializationService.createObjectDataInput(data);

        return () -> uncheckCall(() -> {
            Object key = in.readObject();
            if (key == SnapshotDataValueTerminator.INSTANCE) {
                in.close();
                return null;
            }
            Object value = in.readObject();
            return key instanceof BroadcastKey
                    ? new BroadcastEntry(key, value)
                    : entry(key, value);
        });
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        return flatMapper.tryProcess(((Entry<Integer, byte[]>) item).getValue());
    }
}
