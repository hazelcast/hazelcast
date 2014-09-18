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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * DataSerializable is a serialization method as an alternative to standard Java serialization.
 * DataSerializable is very similar to {@link java.io.Externalizable} and relies on reflection to create
 * instances using classnames.
 *
 * @see com.hazelcast.nio.serialization.IdentifiedDataSerializable
 * @see com.hazelcast.nio.serialization.Portable
 * @see com.hazelcast.nio.serialization.VersionedPortable
 */
public interface DataSerializable {

    /**
     * Writes object fields to output stream
     *
     * @param out output
     * @throws IOException
     */
    void writeData(ObjectDataOutput out) throws IOException;

    /**
     * Reads fields from the input stream
     *
     * @param in input
     * @throws IOException
     */
    void readData(ObjectDataInput in) throws IOException;

}
