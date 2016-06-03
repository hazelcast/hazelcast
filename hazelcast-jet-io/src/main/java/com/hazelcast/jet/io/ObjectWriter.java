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

package com.hazelcast.jet.io;

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Represents abstract writer of Java object into binary format;
 *
 * @param <T> - type of the object;
 */
public interface ObjectWriter<T> {
    /**
     * Writes type of the object;
     *
     * @param object              - object to write;
     * @param objectDataOutput    - source for binary object's representation;
     * @param objectWriterFactory - factory for the object writers;
     * @throws IOException if any exception
     */
    void writeType(T object, ObjectDataOutput objectDataOutput, ObjectWriterFactory objectWriterFactory)
            throws IOException;

    /**
     * Write entries of the object as binary representation;
     *
     * @param object              - object to write;
     * @param objectDataOutput    - source for binary object's representation;
     * @param objectWriterFactory - factory for the object writers;
     * @throws IOException if any exception
     */
    void writePayLoad(T object, ObjectDataOutput objectDataOutput, ObjectWriterFactory objectWriterFactory)
            throws IOException;

    /**
     * Write type and entries of the object;
     *
     * @param object              - object to write;
     * @param objectDataOutput    - source for binary object's representation;
     * @param objectWriterFactory - factory for the object writers;
     * @throws IOException if any exception
     */
    void write(T object, ObjectDataOutput objectDataOutput, ObjectWriterFactory objectWriterFactory)
            throws IOException;
}
