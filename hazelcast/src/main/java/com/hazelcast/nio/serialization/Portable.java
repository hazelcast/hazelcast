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

import java.io.IOException;

/**
 * Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is
 * created by a registered {@link com.hazelcast.nio.serialization.PortableFactory}.
 *
 * <p>
 *
 * Portable serialization that have the following advantages:
 * <ul>
 *     <li>Support multiversion of the same object type.
 *     (See {@link com.hazelcast.config.SerializationConfig#setPortableVersion(int)})</li>
 *     <li>Fetching individual fields without having to rely on reflection.</li>
 *     <li>Querying and indexing support without de-serialization and/or reflection.</li>
 * </ul>
 *
 * @see com.hazelcast.nio.serialization.PortableFactory
 * @see com.hazelcast.nio.serialization.PortableWriter
 * @see com.hazelcast.nio.serialization.PortableReader
 * @see com.hazelcast.nio.serialization.ClassDefinition
 * @see com.hazelcast.nio.serialization.DataSerializable
 * @see com.hazelcast.nio.serialization.IdentifiedDataSerializable
 * @see com.hazelcast.config.SerializationConfig
 */

public interface Portable {

    /**
     * Returns PortableFactory id for this portable class
     * @return factory id
     */
    int getFactoryId();

    /**
     * Returns class identifier for this portable class. Class id should be unique per PortableFactory.
     * @return class id
     */
    int getClassId();

    /**
     * Serialize this portable object using PortableWriter
     *
     * @param writer PortableWriter
     * @throws IOException
     */
    void writePortable(PortableWriter writer) throws IOException;

    /**
     * Read portable fields using PortableReader
     *
     * @param reader PortableReader
     * @throws IOException
     */
    void readPortable(PortableReader reader) throws IOException;
}
