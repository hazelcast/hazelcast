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
import com.hazelcast.nio.serialization.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

public class ListSerializerHook implements SerializerHook<List> {

    @Override
    public Class<List> getSerializationType() {
        return List.class;
    }

    @Override
    public Serializer createSerializer() {
        return new ListStreamSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }

    public static class ListStreamSerializer implements StreamSerializer<List> {

        @Override
        public void write(ObjectDataOutput out, List object) throws IOException {
            out.writeBoolean(object != null);
            if (object != null) {
                int size = object.size();
                out.writeInt(size);
                if (object instanceof RandomAccess) {
                    boolean hasDataSerializable = false;
                    if (!object.isEmpty()) {
                        hasDataSerializable = object.get(0) instanceof DataSerializable;
                    }
                    out.writeBoolean(hasDataSerializable);
                    for (int i = 0; i < size; i++) {
                        if (hasDataSerializable) {
                            ((DataSerializable) object.get(i)).writeData(out);
                        } else {
                            out.writeObject(object.get(i));
                        }
                    }
                } else {
                    Iterator iterator = object.iterator();
                    boolean hasDataSerializable = false;
                    if (!iterator.hasNext()) {
                        hasDataSerializable = iterator.next() instanceof DataSerializable;
                    }
                    out.writeBoolean(hasDataSerializable);
                    iterator = object.iterator();
                    while (iterator.hasNext()) {
                        if (hasDataSerializable) {
                            ((DataSerializable) iterator.next()).writeData(out);
                        } else {
                            out.writeObject(iterator.next());
                        }
                    }
                }
            }
        }

        @Override
        public ArrayList read(ObjectDataInput in) throws IOException {
            if (in.readBoolean()) {
                int size = in.readInt();
                ArrayList result = new ArrayList(size);
                boolean hasDataSerializable = in.readBoolean();
                if (hasDataSerializable) {
                    for (int i = 0; i < size; i++) {
                        Object o = new Object();
                        ((DataSerializable) o).readData(in);
                        result.add(i, o);
                    }
                } else {
                    for (int i = 0; i < size; i++) {
                        result.add(i, in.readObject());
                    }
                }
                return result;
            }
            return null;
        }

        @Override
        public int getTypeId() {
            return SerializationConstants.AUTO_TYPE_LIST;
        }

        @Override
        public void destroy() {
        }
    }
}
