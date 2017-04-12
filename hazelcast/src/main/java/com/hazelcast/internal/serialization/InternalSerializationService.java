/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Disposable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.nio.ByteOrder;

public interface InternalSerializationService extends SerializationService, Disposable {

    byte VERSION_1 = 1;

    /**
     * Writes the obj to a byte array. This call is exactly the same as calling {@link #toData(Object)} and
     * then calling {@link Data#toByteArray()}. But it doesn't force a HeapData object being created.
     */
    byte[] toBytes(Object obj);

    /**
     * Writes an object to a byte-array.
     *
     * It allows for configurable padding on the left.
     *
     * The padded bytes are not zero'd out since they will be written by the caller. Zero'ing them out would be waste of
     * time.
     *
     * If you want to convert an object to a Data (or its byte representation) then you want to have the partition hash, because
     * that is part of the Data-definition.
     *
     * But if you want to serialize an object to a byte-array and don't care for the Data partition-hash, the hash can be
     * disabled.
     */
    byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash);

    void writeObject(ObjectDataOutput out, Object obj);

    <T> T readObject(ObjectDataInput in);

    <T> T readObject(ObjectDataInput in, Class aClass);

    void disposeData(Data data);

    BufferObjectDataInput createObjectDataInput(byte[] data);

    BufferObjectDataInput createObjectDataInput(Data data);

    BufferObjectDataOutput createObjectDataOutput(int size);

    BufferObjectDataOutput createObjectDataOutput();

    PortableReader createPortableReader(Data data) throws IOException;

    PortableContext getPortableContext();

    ClassLoader getClassLoader();

    ByteOrder getByteOrder();

    byte getVersion();

}
