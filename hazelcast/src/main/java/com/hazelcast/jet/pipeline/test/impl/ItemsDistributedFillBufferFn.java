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

package com.hazelcast.jet.pipeline.test.impl;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Iterator;

public class ItemsDistributedFillBufferFn<T>
        implements BiConsumerEx<Context, SourceBuffer<T>>, IdentifiedDataSerializable {

    private Iterable<? extends T> items;

    // for deserialization
    public ItemsDistributedFillBufferFn() { }

    public ItemsDistributedFillBufferFn(Iterable<? extends T> items) {
        this.items = items;
    }

    @Override
    public void acceptEx(Context ctx, SourceBuffer<T> buf) throws Exception {
        Iterator<? extends T> iterator = items.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            T item = iterator.next();
            if (i % ctx.totalParallelism() == ctx.globalProcessorIndex()) {
                buf.add(item);
            }
        }
        buf.close();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetDataSerializerHook.TEST_SOURCES_ITEMS_DISTRIBUTED_FILL_BUFFER_FN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(items);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        items = in.readObject();
    }
}
