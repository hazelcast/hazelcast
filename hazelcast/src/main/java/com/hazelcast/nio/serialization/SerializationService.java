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

package com.hazelcast.nio.serialization;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;

import java.io.IOException;

/**
 * A service that provides access to serialization functionality.
 *
 * For Hazelcast engineers: if you are looking for more options, please check the
 * {@link com.hazelcast.internal.serialization.InternalSerializationService}.
 */
public interface SerializationService {

    <B extends Data> B toData(Object obj);

    <B extends Data> B toData(Object obj, PartitioningStrategy strategy);

    <T> T toObject(Object data);

    PortableContext getPortableContext();

    ClassLoader getClassLoader();

    ManagedContext getManagedContext();

    PortableReader createPortableReader(Data data) throws IOException;
}
