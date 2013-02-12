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
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_DATA;

/**
 * @mdogan 6/19/12
 */
public final class DataSerializer implements TypeSerializer<DataSerializable> {

    private static final Map<Integer, DataSerializableFactory> factories;

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    static {
        final Map<Integer, DataSerializableFactory> map = new HashMap<Integer, DataSerializableFactory>();
        try {
            final Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID);
            while (hooks.hasNext()) {
                DataSerializerHook hook = hooks.next();
                map.putAll(hook.getFactories());
            }
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        factories = Collections.unmodifiableMap(map);
        factories.values();
        factories.keySet();
        factories.entrySet();
    }

    public int getTypeId() {
        return CONSTANT_TYPE_DATA;
    }

    public final DataSerializable read(ObjectDataInput in) throws IOException {
        final DataSerializable ds;
        final boolean identified = in.readBoolean();
        int id = -1;
        String className = null;
        try {
            if (identified) {
                id = in.readInt();
                final DataSerializableFactory dsf = factories.get(id);
                if (dsf == null) {
                    throw new IllegalArgumentException("No DataSerializer factory for id: " + id);
                }
                ds = dsf.create();
            } else {
                className = in.readUTF();
                ds = (DataSerializable) newInstance(className);
            }
            ds.readData(in);
            return ds;
        } catch (final Exception e) {
            throw new HazelcastSerializationException("Problem while reading DataSerializable " +
                    "id: " + id + ", class: " + className + ", exception: " + e.getMessage(), e);
        }
    }

    public final void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        final boolean identified = obj instanceof IdentifiedDataSerializable;
        out.writeBoolean(identified);
        if (identified) {
            out.writeInt(((IdentifiedDataSerializable) obj).getId());
        } else {
            out.writeUTF(obj.getClass().getName());
        }
        obj.writeData(out);
    }

    public void destroy() {
    }
}
