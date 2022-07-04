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

package com.hazelcast.nio.serialization;

import java.io.IOException;

/**
 * Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is
 * created by a registered {@link com.hazelcast.nio.serialization.PortableFactory}.
 *
 * <p>
 *
 * Portable serialization has the following advantages:
 * <ul>
 *     <li>Support multiple versions of the same object type.
 *     (See {@link com.hazelcast.config.SerializationConfig#setPortableVersion(int)})</li>
 *     <li>Fetching individual fields without having to rely on reflection.</li>
 *     <li>Querying and indexing support without de-serialization and/or reflection.</li>
 * </ul>
 *
 * @see com.hazelcast.nio.serialization.PortableFactory
 * @see PortableWriter
 * @see PortableReader
 * @see ClassDefinition
 * @see com.hazelcast.nio.serialization.DataSerializable
 * @see com.hazelcast.nio.serialization.IdentifiedDataSerializable
 * @see com.hazelcast.config.SerializationConfig
 */

public interface Portable {

    /**
     * Returns PortableFactory ID for this portable class
     * @return factory ID
     */
    int getFactoryId();

    /**
     * Returns class identifier for this portable class. Class ID should be unique per PortableFactory.
     * @return class ID
     */
    int getClassId();

    /**
     * Serialize this portable object using PortableWriter
     *
     * @param writer PortableWriter
     * @throws IOException in case of any exceptional case
     */
    void writePortable(PortableWriter writer) throws IOException;

    /**
     * Read portable fields using PortableReader
     *
     * @param reader PortableReader
     * @throws IOException in case of any exceptional case
     */
    void readPortable(PortableReader reader) throws IOException;
}
