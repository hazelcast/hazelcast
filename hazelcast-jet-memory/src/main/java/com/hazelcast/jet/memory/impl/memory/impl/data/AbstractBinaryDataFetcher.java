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

package com.hazelcast.jet.memory.impl.memory.impl.data;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.memory.impl.util.IOUtil;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.memory.spi.operations.ContainersPull;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.spi.operations.BinaryDataFetcher;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.io.serialization.JetSerializationService;
import com.hazelcast.jet.io.impl.serialization.JetSerializationServiceImpl;

import java.io.IOException;

public abstract class AbstractBinaryDataFetcher<T>
        implements BinaryDataFetcher<T> {
    protected int poolIndex;

    protected int poolSizeMask;

    protected T currentContainer;

    private final IOContext ioContext;

    private final JetDataInput dataInput;

    private final ContainersPull<T> containersPull;

    public AbstractBinaryDataFetcher(
            IOContext ioContext,
            ContainersPull<T> containersPull,
            boolean useBigEndian
    ) {
        this.ioContext = ioContext;
        this.containersPull = containersPull;

        JetSerializationService jetSerializationService =
                new JetSerializationServiceImpl();

        this.dataInput = jetSerializationService.createObjectDataInput(
                null,
                useBigEndian
        );

        this.poolSizeMask = containersPull.size() - 1;
    }

    protected T getNextPooledContainer() {
        int index = poolIndex;
        poolIndex = (poolIndex + 1) & poolSizeMask;
        return containersPull.get(index);
    }

    @Override
    public void fetchRecord(MemoryBlock memoryBlock,
                            long recordAddress,
                            ElementsWriter<T> keyWriter,
                            ElementsWriter<T> valueWriter) throws IOException {
        dataInput.setMemoryManager(memoryBlock);

        dataInput.reset(
                IOUtil.getKeyAddress(recordAddress, memoryBlock),
                IOUtil.getKeyWrittenBytes(recordAddress, memoryBlock)
        );
        readElements(keyWriter);
        dataInput.reset(
                IOUtil.getValueAddress(recordAddress, memoryBlock),
                IOUtil.getValueWrittenBytes(recordAddress, memoryBlock)
        );

        readElements(valueWriter);
    }

    @Override
    public T getCurrentContainer() {
        return currentContainer;
    }

    private void readElements(ElementsWriter<T> writer) throws IOException {
        int elementsCount = dataInput.readInt();

        for (int i = 0; i < elementsCount; i++) {
            byte typeId = dataInput.readByte();

            Object element = ioContext.getDataType(typeId).getObjectReader().read(
                    dataInput,
                    ioContext.getObjectReaderFactory()
            );

            writer.writeNext(element);
        }
    }

    @Override
    public void reset() {
        currentContainer = null;
    }
}
