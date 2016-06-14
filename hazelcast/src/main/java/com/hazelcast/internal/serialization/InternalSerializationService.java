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

package com.hazelcast.internal.serialization;

import com.hazelcast.core.PartitioningStrategy;
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

    byte[] toBytes(Object obj);

    byte[] toBytes(Object obj, PartitioningStrategy strategy);

    void writeObject(ObjectDataOutput out, Object obj);

    <T> T readObject(ObjectDataInput in);

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

    /**
     * Serializes and Deserializes an object to provide a deep-copy through serialization.
     * <p/>
     * If this method is called with null, null is returned.
     *
     * @param obj the object to serialize & deserialize.
     * @return the copied object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when (de)serialization fails.
     */
    <T> T copy(T obj);

}
