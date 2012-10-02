/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.FastDataInputStream;
import com.hazelcast.nio.FastDataOutputStream;
import com.hazelcast.nio.HazelcastSerializationException;
import com.hazelcast.util.ServiceLoader;
import com.hazelcast.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.nio.serialization.SerializationConstants.SERIALIZER_TYPE_DATA;

/**
 * @mdogan 6/19/12
 */
public final class DataSerializer implements TypeSerializer<DataSerializable> {

    private static final Map<String, DataSerializableFactory> factories;

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    static {
        final Map<String, DataSerializableFactory> map = new HashMap<String, DataSerializableFactory>();
        try {
            final Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID);
            while (hooks.hasNext()) {
                DataSerializerHook hook = hooks.next();
                map.putAll(hook.createFactoryMap());
            }
        } catch (Exception e) {
            Util.throwUncheckedException(e);
        }
        factories = Collections.unmodifiableMap(map);
        factories.values();
        factories.keySet();
        factories.entrySet();
    }

    public int getTypeId() {
        return SERIALIZER_TYPE_DATA;
    }

    public final DataSerializable read(final FastDataInputStream in) throws Exception {
        final String className = in.readUTF();
        try {
            DataSerializable ds;
            DataSerializableFactory dsf = factories.get(className);
            if (dsf != null) {
                ds = dsf.create();
            } else {
                ds = (DataSerializable) newInstance(className);
            }
            ds.readData(in);
            return ds;
        } catch (final Exception e) {
            throw new HazelcastSerializationException("Problem while reading DataSerializable class : "
                    + className + ", exception: " + e.getMessage(), e);
        }
    }

    public final void write(final FastDataOutputStream out, final DataSerializable obj) throws Exception {
        out.writeUTF(obj.getClass().getName());
        obj.writeData(out);
    }

    public void destroy() {
    }
}
