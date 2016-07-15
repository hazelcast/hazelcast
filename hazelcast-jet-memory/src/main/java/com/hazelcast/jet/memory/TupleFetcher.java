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
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.tuple.Tuple2;
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
    protected final Tuple2 tuple;
    private final IOContext ioContext;
    private final JetDataInput dataInput;

    public TupleFetcher(IOContext ioContext, Tuple2 tuple, boolean useBigEndian) {
        this.ioContext = ioContext;
        this.tuple = tuple;
        JetSerializationService jetSerializationService = new JetSerializationServiceImpl();
        this.dataInput = jetSerializationService.createObjectDataInput(null, useBigEndian);
    }

    public void fetch(MemoryBlock memoryBlock, long recordAddress) {
        dataInput.setMemoryManager(memoryBlock);
        final MemoryAccessor accessor = memoryBlock.getAccessor();
        dataInput.reset(addressOfKeyBlockAt(recordAddress), sizeOfKeyBlockAt(recordAddress, accessor));
        tuple.set0(readObject());
        dataInput.reset(addrOfValueBlockAt(recordAddress, accessor), sizeOfValueBlockAt(recordAddress, accessor));
        tuple.set1(readObject());
    }

    public Tuple2 tuple() {
        return tuple;
    }

    private Object readObject() {
        try {
            byte typeId = dataInput.readByte();
            return ioContext.getDataType(typeId).getObjectReader()
                            .read(dataInput, ioContext.getObjectReaderFactory());
        } catch (IOException e) {
            throw new JetMemoryException(e);
        }
    }
}
