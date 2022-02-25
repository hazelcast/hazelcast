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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * DataSerializable is a serialization method alternative to standard Java
 * serialization. DataSerializable is very similar to {@link java.io.Externalizable}
 * and relies on reflection to create instances using class names.
 * <p>
 * Conforming classes must provide a no-arguments constructor to facilitate the
 * creation of their instances during the deserialization. Anonymous, local and
 * non-static member classes can't satisfy this requirement since their
 * constructors are always accepting an instance of the enclosing class as an
 * implicit argument, therefore they must be avoided.
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
     * @throws IOException if an I/O error occurs. In particular,
     *                     an <code>IOException</code> may be thrown if the
     *                     output stream has been closed.
     */
    void writeData(ObjectDataOutput out) throws IOException;

    /**
     * Reads fields from the input stream
     *
     * @param in input
     * @throws IOException if an I/O error occurs. In particular,
     *                     an <code>IOException</code> may be thrown if the
     *                     input stream has been closed.
     */
    void readData(ObjectDataInput in) throws IOException;
}
