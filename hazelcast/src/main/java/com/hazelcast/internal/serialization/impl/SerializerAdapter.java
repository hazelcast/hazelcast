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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.TypedStreamDeserializer;

import java.io.IOException;

public interface SerializerAdapter {

    void write(ObjectDataOutput out, Object object) throws IOException;

    Object read(ObjectDataInput in) throws IOException;

    int getTypeId();

    void destroy();

    Serializer getImpl();

    default Object read(ObjectDataInput in, Class aClass) throws IOException {
        throw new HazelcastSerializationException(this + " is not implementing the " + TypedStreamDeserializer.class
                + " interface. Please implement this interface to deserialize for class " + aClass);
    }
}
