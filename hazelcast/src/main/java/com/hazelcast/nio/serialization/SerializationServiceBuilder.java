/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;

import java.nio.ByteOrder;

public interface SerializationServiceBuilder {

    SerializationServiceBuilder setVersion(int version);

    SerializationServiceBuilder setClassLoader(ClassLoader classLoader);

    SerializationServiceBuilder setConfig(SerializationConfig config);

    SerializationServiceBuilder addDataSerializableFactory(int id, DataSerializableFactory factory);

    SerializationServiceBuilder addPortableFactory(int id, PortableFactory factory);

    SerializationServiceBuilder addClassDefinition(ClassDefinition cd);

    SerializationServiceBuilder setCheckClassDefErrors(boolean checkClassDefErrors);

    SerializationServiceBuilder setManagedContext(ManagedContext managedContext);

    SerializationServiceBuilder setUseNativeByteOrder(boolean useNativeByteOrder);

    SerializationServiceBuilder setByteOrder(ByteOrder byteOrder);

    SerializationServiceBuilder setHazelcastInstance(HazelcastInstance hazelcastInstance);

    SerializationServiceBuilder setEnableCompression(boolean enableCompression);

    SerializationServiceBuilder setEnableSharedObject(boolean enableSharedObject);

    SerializationServiceBuilder setAllowUnsafe(boolean allowUnsafe);

    SerializationServiceBuilder setPartitioningStrategy(PartitioningStrategy partitionStrategy);

    SerializationServiceBuilder setInitialOutputBufferSize(int initialOutputBufferSize);

    SerializationService build();
}
