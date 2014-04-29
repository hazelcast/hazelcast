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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Iterator;

/**
 * This class is the auto registered serializer hook for a {@link java.util.LinkedList}.
 */
public class LinkedListSerializerHook implements SerializerHook<LinkedList> {

    @Override
    public Class<LinkedList> getSerializationType() {
        return LinkedList.class;
    }

    @Override
    public Serializer createSerializer() {
        return new LinkedListStreamSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }

    /**
     * The {@link java.util.LinkedList} serializer
     */
    public static class LinkedListStreamSerializer implements StreamSerializer<LinkedList> {

        @Override
        public void write(ObjectDataOutput out, LinkedList object) throws IOException {
            out.writeBoolean(object != null);
            if (object != null) {
                int size = object.size();
                out.writeInt(size);
                Iterator iterator = object.iterator();
                while (iterator.hasNext()) {
                    out.writeObject(iterator.next());
                }
            }
        }

        @Override
        public LinkedList read(ObjectDataInput in) throws IOException {
            if (in.readBoolean()) {
                int size = in.readInt();
                LinkedList result = new LinkedList();
                for (int i = 0; i < size; i++) {
                    result.add(i, in.readObject());
                }
                return result;
            }
            return null;
        }

        @Override
        public int getTypeId() {
            return SerializationConstants.AUTO_TYPE_LINKED_LIST;
        }

        @Override
        public void destroy() {
        }
    }
}
