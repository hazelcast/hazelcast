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

package com.hazelcast.nio.serialization.serializers;

import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;

import java.util.HashMap;

public class HashMapSerializerHook
        implements SerializerHook<HashMap> {

    @Override
    public Class<HashMap> getSerializationType() {
        return HashMap.class;
    }

    @Override
    public Serializer createSerializer() {
        return new AbstractMapSerializer<HashMap>() {

            @Override
            //CHECKSTYLE:OFF
            //Deactivated since implementation classes of interfaces are actually not
            //allowed as return types by checkstyle but I want it anyways :)
            protected HashMap newMap(int size) {
                return new HashMap(size);
            }
            //CHECKSTYLE:ON

            @Override
            public int getTypeId() {
                return SerializationConstants.AUTO_TYPE_HASH_MAP;
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
