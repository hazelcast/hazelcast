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

package com.hazelcast.jet.memory.impl.util;

import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordHeaderLayOut;
import com.hazelcast.nio.Bits;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.spi.operations.ElementsWriter;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.RecordLayout;

import java.io.IOException;

public final class IOUtil {
    private IOUtil() {
    }

    public static <T> void writeRecord(T record,
                                       ElementsReader<T> keyReader,
                                       ElementsReader<T> valueReader,
                                       IOContext ioContext,
                                       JetDataOutput output,
                                       MemoryManager memoryManager) throws IOException {
        output.clear();
        output.setMemoryManager(memoryManager);
        processReader(record, keyReader, ioContext, output, memoryManager.getAccessor());
        processReader(record, valueReader, ioContext, output, memoryManager.getAccessor());
    }

    public static <T> void processReader(T record,
                                         ElementsReader<T> reader,
                                         IOContext ioContext,
                                         JetDataOutput output,
                                         MemoryAccessor memoryAccessor) throws IOException {
        reader.setSource(record);
        // Just reserve long to write size
        long position = output.position();
        output.stepOn(Bits.LONG_SIZE_IN_BYTES);
        long pointer = output.getPointer();
        long writtenBytes = output.getWrittenSize();

        // Elements count
        output.writeInt(reader.size());

        while (reader.hasNext()) {
            writeObject(reader.next(), ioContext, output);
        }

        memoryAccessor.putLong(
                pointer + position,
                output.getWrittenSize() - writtenBytes
        );
    }

    public static <T> void processWriter(T record,
                                         long recordAddress,
                                         ElementsWriter<T> keyWriter,
                                         ElementsWriter<T> valueWriter,
                                         IOContext ioContext,
                                         JetDataInput input,
                                         MemoryAccessor memoryAccessor) throws IOException {
        keyWriter.setSource(record);
        valueWriter.setSource(record);

        input.reset(
                recordAddress,
                IOUtil.getRecordSize(
                        recordAddress,
                        memoryAccessor
                )
        );

        input.readLong();
        processElements(keyWriter, ioContext, input);
        input.readLong();
        processElements(valueWriter, ioContext, input);
    }

    private static <T> void processElements(
            ElementsWriter<T> writer,
            IOContext ioContext,
            JetDataInput input) throws IOException {
        int elementsSize =
                input.readInt();

        for (int i = 0; i < elementsSize; i++) {
            readObject(writer, ioContext, input);
        }
    }

    private static <T> void readObject(ElementsWriter<T> writer,
                                       IOContext ioContext,
                                       JetDataInput input) throws IOException {
        byte typeID = input.readByte();
        writer.writeNext(
                ioContext.getDataType(typeID).getObjectReader().read(
                        input,
                        ioContext.getObjectReaderFactory()
                )
        );
    }

    @SuppressWarnings("unchecked")
    public static void writeObject(Object object,
                                   IOContext ioContext,
                                   JetDataOutput output) throws IOException {
        ioContext.getDataType(object).getObjectWriter().write(
                object,
                output,
                ioContext.getObjectWriterFactory()
        );
    }


    public static long getLong(long recordAddress, long offset, MemoryAccessor memoryAccessor) {
        if (recordAddress == MemoryUtil.NULL_VALUE) {
            return MemoryUtil.NULL_VALUE;
        }

        return memoryAccessor.getLong(
                recordAddress + offset
        );
    }

    public static void setLong(long recordAddress, long offset, long value, MemoryAccessor memoryAccessor) {
        if (recordAddress == MemoryUtil.NULL_VALUE) {
            return;
        }

        memoryAccessor.putLong(recordAddress + offset, value);
    }

    public static void setShort(long recordAddress, long offset, short value, MemoryAccessor memoryAccessor) {
        if (recordAddress == MemoryUtil.NULL_VALUE) {
            return;
        }

        memoryAccessor.putShort(recordAddress + offset, value);
    }

    public static byte getByte(long recordAddress, long offset, MemoryAccessor memoryAccessor) {
        if (recordAddress == MemoryUtil.NULL_VALUE) {
            return Util.ZERO;
        }

        return memoryAccessor.getByte(
                recordAddress +
                        offset
        );
    }

    public static void setByte(long recordAddress,
                               long offset,
                               byte value,
                               MemoryAccessor memoryAccessor) {
        if (recordAddress == MemoryUtil.NULL_VALUE) {
            return;
        }

        memoryAccessor.putByte(recordAddress + offset, value);
    }

    public static long getKeyAddress(long recordAddress,
                                     MemoryAccessor memoryAccessor) {
        return recordAddress + RecordLayout.KEY_POINTER_OFFSET;
    }

    public static long getKeyWrittenBytes(long recordAddress, MemoryAccessor memoryAccessor) {
        return getLong(recordAddress, RecordLayout.KEY_WRITTEN_BYTES_OFFSET, memoryAccessor);
    }

    public static long getValueAddress(long recordAddress, MemoryAccessor memoryAccessor) {
        long keySize = getKeyWrittenBytes(recordAddress, memoryAccessor);
        return recordAddress + (Bits.LONG_SIZE_IN_BYTES << 1) + keySize;
    }

    public static long getValueWrittenBytes(long recordAddress, MemoryAccessor memoryAccessor) {
        long keySize = getKeyWrittenBytes(recordAddress, memoryAccessor);
        return getLong(recordAddress, Bits.LONG_SIZE_IN_BYTES + keySize, memoryAccessor);
    }

    public static long getRecordSource(long recordAddress, MemoryAccessor memoryAccessor) {
        long recordSize = getRecordSize(recordAddress, memoryAccessor);
        return getLong(recordAddress, recordSize + RecordHeaderLayOut.SOURCE_OFFSET, memoryAccessor);
    }

    public static long getRecordSize(long recordAddress,
                                     MemoryAccessor memoryAccessor) {
        return
                getKeyWrittenBytes(recordAddress, memoryAccessor)
                        +
                        getValueWrittenBytes(recordAddress, memoryAccessor)
                        +
                        (Bits.LONG_SIZE_IN_BYTES << 1);
    }
}