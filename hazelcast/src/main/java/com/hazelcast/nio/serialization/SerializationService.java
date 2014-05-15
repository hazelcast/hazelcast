/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;

public interface SerializationService {

    Data toData(Object obj);

    Data toData(Object obj, PartitioningStrategy strategy);

    <T> T toObject(Object data);

    void writeObject(ObjectDataOutput out, Object obj);

    Object readObject(ObjectDataInput in);

    BufferObjectDataInput createObjectDataInput(byte[] data);

    BufferObjectDataInput createObjectDataInput(Data data);

    BufferObjectDataOutput createObjectDataOutput(int size);

    ObjectDataOutputStream createObjectDataOutputStream(OutputStream out);

    ObjectDataInputStream createObjectDataInputStream(InputStream in);

    ObjectDataOutputStream createObjectDataOutputStream(OutputStream out, ByteOrder order);

    ObjectDataInputStream createObjectDataInputStream(InputStream in, ByteOrder order);

    void register(Class type, Serializer serializer);

    void registerGlobal(Serializer serializer);

    SerializationContext getSerializationContext();

    PortableReader createPortableReader(Data data);

    ClassLoader getClassLoader();

    ManagedContext getManagedContext();

}
