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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

/**
 * The {@link java.util.LinkedList} serializer
 */
public class LinkedListStreamSerializer implements StreamSerializer<LinkedList> {

    @Override
    public void write(ObjectDataOutput out, LinkedList linkedList) throws IOException {
        int size = linkedList == null ? NULL_ARRAY_LENGTH : linkedList.size();
        out.writeInt(size);
        if (size > 0) {
            Iterator iterator = linkedList.iterator();
            while (iterator.hasNext()) {
                out.writeObject(iterator.next());
            }
        }
    }

    @Override
    public LinkedList read(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        LinkedList result = null;
        if (size > NULL_ARRAY_LENGTH) {
            result = new LinkedList();
            for (int i = 0; i < size; i++) {
                result.add(i, in.readObject());
            }
        }
        return result;
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_LINKED_LIST;
    }

    @Override
    public void destroy() {
    }
}
