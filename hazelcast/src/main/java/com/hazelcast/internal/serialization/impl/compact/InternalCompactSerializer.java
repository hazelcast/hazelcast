/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import java.io.IOException;

public interface InternalCompactSerializer<T, Reader extends CompactReader> {

    /**
     * @param in     reader to read fields of an object
     * @return the object created as a result of read method
     * @throws IOException
     */
    T read(Reader in) throws IOException;

    /**
     * @param out    CompactWriter to serialize the fields onto
     * @param object to be serialized
     * @throws IOException
     */
    void write(CompactWriter out, T object) throws IOException;
}
