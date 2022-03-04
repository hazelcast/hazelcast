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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Default {@link BufferPool} implementation.
 * <p>
 * This class is designed to that a subclass can be made. This is done for the Enterprise version.
 */
public class BufferPoolImpl implements BufferPool {
    static final int MAX_POOLED_ITEMS = 3;

    protected final InternalSerializationService serializationService;

    // accessible for testing.
    final Queue<BufferObjectDataOutput> outputQueue = new ArrayDeque<>(MAX_POOLED_ITEMS);
    final Queue<BufferObjectDataInput> inputQueue = new ArrayDeque<>(MAX_POOLED_ITEMS);

    public BufferPoolImpl(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public BufferObjectDataOutput takeOutputBuffer() {
        BufferObjectDataOutput out = outputQueue.poll();
        if (out == null) {
            out = serializationService.createObjectDataOutput();
        }
        return out;
    }

    @Override
    public void returnOutputBuffer(BufferObjectDataOutput out) {
        if (out == null) {
            return;
        }

        out.clear();

        tryOffer(outputQueue, out);
    }

    @Override
    public BufferObjectDataInput takeInputBuffer(Data data) {
        BufferObjectDataInput in = inputQueue.poll();
        if (in == null) {
            in = serializationService.createObjectDataInput((byte[]) null);
        }
        in.init(data.toByteArray(), HeapData.DATA_OFFSET);
        return in;
    }

    @Override
    public void returnInputBuffer(BufferObjectDataInput in) {
        if (in == null) {
            return;
        }

        in.clear();

        tryOffer(inputQueue, in);
    }

    private static <C> void tryOffer(Queue<C> queue, C item) {
        if (queue.size() == MAX_POOLED_ITEMS) {
            return;
        }

        queue.offer(item);
    }
}
