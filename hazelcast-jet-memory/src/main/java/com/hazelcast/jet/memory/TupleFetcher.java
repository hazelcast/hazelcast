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

package com.hazelcast.jet.memory;


import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.memory.serialization.MemoryDataInput;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;

import java.io.IOException;

import static com.hazelcast.jet.memory.util.JetIoUtil.addrOfValueBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.addressOfKeyBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfKeyBlockAt;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfValueBlockAt;

/**
 * Deserializes data held by a {@code MemoryManager} and puts it into a {@code Tuple}.
 */
public class TupleFetcher {
    protected final Pair pair;
    private final MemoryDataInput dataInput;

    public TupleFetcher(SerializationOptimizer optimizer, Pair pair, boolean useBigEndian) {
        this.pair = pair;
        this.dataInput = new MemoryDataInput(null, optimizer, useBigEndian);
    }

    public void fetch(MemoryBlock memoryBlock, long recordAddress) {
        dataInput.setMemoryManager(memoryBlock);
        final MemoryAccessor accessor = memoryBlock.getAccessor();
        dataInput.reset(addressOfKeyBlockAt(recordAddress), sizeOfKeyBlockAt(recordAddress, accessor));
        pair.setKey(readObject());
        dataInput.reset(addrOfValueBlockAt(recordAddress, accessor), sizeOfValueBlockAt(recordAddress, accessor));
        pair.setValue(readObject());
    }

    public Pair pair() {
        return pair;
    }

    private Object readObject() {
        try {
            return dataInput.readOptimized();
        } catch (IOException e) {
            throw new JetMemoryException(e);
        }
    }
}
