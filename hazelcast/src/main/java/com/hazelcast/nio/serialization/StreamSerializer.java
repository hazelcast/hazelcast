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
 * A base class for custom serialization. User can register custom serializer like following:
 * <p/>
 * <pre>
 *     final SerializerConfig serializerConfig = new SerializerConfig();
 *     serializerConfig.setImplementation(new StreamSerializer<Person>() {
 *          public int getTypeId() {
 *
 *          }
 *
 *          public void destroy() {
 *
 *          }
 *
 *          public void write(ObjectDataOutput out, Person object) throws IOException {
 *
 *          }
 *
 *          public Person read(ObjectDataInput in) throws IOException {
 *
 *          });
 *
 *     serializerConfig.setTypeClass(Person.class);
 *     config.getSerializationConfig().addSerializerConfig(serializerConfig);
 *
 * </pre>
 * <p/>
 * There is another class with byte arrays can be used instead ıf this interface
 * see {@link com.hazelcast.nio.serialization.ByteArraySerializer}.
 * <p/>
 * C++ and C# clients also have compatible methods so that with custom serialization client can also be used
 * <p/>
 * Note that read and write methods should be compatible
 *
 * @param <T> type of the serialized object
 */
public interface StreamSerializer<T> extends Serializer {

    /**
     * This method writes object to ObjectDataOutput
     *
     * @param out    ObjectDataOutput stream that object will be written to
     * @param object that will be written to out
     * @throws IOException in case of failure to write
     */

    void write(ObjectDataOutput out, T object) throws IOException;

    /**
     * Reads object from objectDataInputStream
     *
     * @param in ObjectDataInput stream that object will read from
     * @return read object
     * @throws IOException in case of failure to read
     */
    T read(ObjectDataInput in) throws IOException;
}
