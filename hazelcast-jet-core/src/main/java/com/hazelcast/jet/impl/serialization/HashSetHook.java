/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.HashSet;

public final class HashSetHook implements SerializerHook<HashSet> {

    @Override
    public Class<HashSet> getSerializationType() {
        return HashSet.class;
    }

    @Override
    @SuppressWarnings("checkstyle:anoninnerlength")
    public Serializer createSerializer() {
        return new StreamSerializer<HashSet>() {

            @Override
            public int getTypeId() {
                return SerializerHookConstants.HASH_SET;
            }

            @Override
            public void destroy() {

            }

            @Override
            @SuppressWarnings("checkstyle:illegaltype")
            public void write(ObjectDataOutput out, HashSet set) throws IOException {
                out.writeInt(set.size());
                for (Object o : set) {
                    out.writeObject(o);
                }
            }

            @Override
            @SuppressWarnings("checkstyle:illegaltype")
            public HashSet read(ObjectDataInput in) throws IOException {
                int length = in.readInt();
                HashSet set = new HashSet();
                for (int i = 0; i < length; i++) {
                    //noinspection unchecked
                    set.add(in.readObject());
                }
                return set;
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
