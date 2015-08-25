/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.Serializer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;

public interface InternalSerializationService extends SerializationService {

    byte[] toBytes(Object obj);

    byte[] toBytes(Object obj, PartitioningStrategy strategy);

    void writeObject(ObjectDataOutput out, Object obj);

    <T> T readObject(ObjectDataInput in);

    void writeData(ObjectDataOutput out, Data data);

    <B extends Data> B readData(ObjectDataInput in);

    void disposeData(Data data);

    BufferObjectDataInput createObjectDataInput(byte[] data);

    BufferObjectDataInput createObjectDataInput(Data data);

    BufferObjectDataOutput createObjectDataOutput(int size);

    BufferObjectDataOutput createObjectDataOutput();

    ObjectDataOutputStream createObjectDataOutputStream(OutputStream out);

    ObjectDataInputStream createObjectDataInputStream(InputStream in);

    void register(Class type, Serializer serializer);

    void registerGlobal(Serializer serializer);

    PortableContext getPortableContext();

    ManagedContext getManagedContext();

    ByteOrder getByteOrder();

    void destroy();
}
