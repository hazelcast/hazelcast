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

import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.nio.serialization.SerializationConstants.CONSTANT_TYPE_DATA;

final class DataSerializer implements StreamSerializer<DataSerializable> {

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private final Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();

    DataSerializer(Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories, ClassLoader classLoader) {
        try {
            final Iterator<DataSerializerHook> hooks = ServiceLoader.iterator(DataSerializerHook.class, FACTORY_ID, classLoader);
            while (hooks.hasNext()) {
                DataSerializerHook hook = hooks.next();
                final DataSerializableFactory factory = hook.createFactory();
                if (factory != null) {
                    register(hook.getFactoryId(), factory);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        if (dataSerializableFactories != null) {
            for (Map.Entry<Integer, ? extends DataSerializableFactory> entry : dataSerializableFactories.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    private void register(int factoryId, DataSerializableFactory factory) {
        final DataSerializableFactory current = factories.get(factoryId);
        if (current != null) {
            if (current.equals(factory)) {
                Logger.getLogger(getClass()).warning("DataSerializableFactory[" + factoryId + "] is already registered! Skipping "
                        + factory);
            } else {
                throw new IllegalArgumentException("DataSerializableFactory[" + factoryId + "] is already registered! "
                        + current + " -> " + factory);
            }
        } else {
            factories.put(factoryId, factory);
        }
    }

    public int getTypeId() {
        return CONSTANT_TYPE_DATA;
    }

    public DataSerializable read(ObjectDataInput in) throws IOException {
        final DataSerializable ds;
        final boolean identified = in.readBoolean();
        int id = 0;
        int factoryId = 0;
        String className = null;
        try {
            if (identified) {
                factoryId = in.readInt();
                final DataSerializableFactory dsf = factories.get(factoryId);
                if (dsf == null) {
                    throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
                }
                id = in.readInt();
                ds = dsf.create(id);
                if (ds == null) {
                    throw new HazelcastSerializationException(dsf
                            + " is not be able to create an instance for id: " + id + " on factoryId: " + factoryId);
                }
                // TODO: @mm - we can check if DS class is final.
            } else {
                className = in.readUTF();
                ds = ClassLoaderUtil.newInstance(in.getClassLoader(), className);
            }
            ds.readData(in);
            return ds;
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof HazelcastSerializationException) {
                throw (HazelcastSerializationException) e;
            }
            throw new HazelcastSerializationException("Problem while reading DataSerializable, namespace: "
                    + factoryId
                    + ", id: " + id
                    + ", class: '" + className+"'"
                    + ", exception: " + e.getMessage(), e);
        }
    }

    public void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        final boolean identified = obj instanceof IdentifiedDataSerializable;
        out.writeBoolean(identified);
        if (identified) {
            final IdentifiedDataSerializable ds = (IdentifiedDataSerializable) obj;
            out.writeInt(ds.getFactoryId());
            out.writeInt(ds.getId());
        } else {
            out.writeUTF(obj.getClass().getName());
        }
        obj.writeData(out);
    }

    public void destroy() {
        factories.clear();
    }
}
