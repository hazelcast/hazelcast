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
 * For sample usage custom serialization and other way of custom serialization
 * see {@link com.hazelcast.nio.serialization.StreamSerializerAdapter}.
 *
 *  Note that read and write methods should be compatible
 *
 * @param <T> type of serialized object
 */
public interface ByteArraySerializer<T> extends Serializer {

    /**
     * Converts given object to byte array
     *
     * @param object that will be serialized
     * @return byte array that object is serialized into
     * @throws IOException
     */
    byte[] write(T object) throws IOException;

    /**
     * Converts given byte array to object
     *
     * @param buffer that object will be read from
     * @return deserialized object
     * @throws IOException
     */
    T read(byte[] buffer) throws IOException;
}
