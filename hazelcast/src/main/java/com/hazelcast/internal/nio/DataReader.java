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

package com.hazelcast.internal.nio;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.Data;

import java.io.DataInput;
import java.io.IOException;

/**
 * Extends {@link DataInput} with ability to read {@link Data} from the input stream.
 */
public interface DataReader extends DataInput {

    /**
     * @return data read
     * @throws IOException if it reaches end of file before finish reading
     */
    Data readData() throws IOException;

    /**
     * Reads to stored Data as an object instead of a Data instance.
     * <p>
     * The reason this method exists is that in some cases {@link Data} is stored on serialization, but on deserialization
     * the actual object instance is needed. Getting access to the {@link Data} is easy by calling the {@link #readData()}
     * method. But de-serializing the {@link Data} to an object instance is impossible because there is no reference to the
     * {@link SerializationService}.
     *
     * @param <T> type of the object to be read
     * @return the read Object
     * @throws IOException if it reaches end of file before finish reading
     */
    <T> T readDataAsObject() throws IOException;
}
