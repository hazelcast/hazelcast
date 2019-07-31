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
import java.util.Map;

import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

/**
 * The {@link Map} serializer
 */
public abstract class AbstractMapStreamSerializer implements StreamSerializer<Map> {

    @Override
    public void write(ObjectDataOutput out, Map map) throws IOException {
        int size = map == null ? NULL_ARRAY_LENGTH : map.size();
        out.writeInt(size);
        if (size > 0) {
            beforeSerializeEntries(out, map);

            Iterator iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }

    @Override
    public Map read(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map result = null;
        if (size > NULL_ARRAY_LENGTH) {
            result = createMap(in, size);
            for (int i = 0; i < size; i++) {
                result.put(in.readObject(), in.readObject());
            }
        }
        return result;
    }

    @Override
    public void destroy() {
    }

    protected abstract Map createMap(ObjectDataInput in, int size)
            throws IOException;

    /**
     * Any derived serializer can do any checks desired by overriding this method.
     */
    protected void beforeSerializeEntries(ObjectDataOutput out, Map map)
            throws IOException {
    }
}
