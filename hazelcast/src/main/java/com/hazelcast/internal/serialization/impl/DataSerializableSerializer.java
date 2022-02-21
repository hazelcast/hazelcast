/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.TypedDataSerializable;
import com.hazelcast.nio.serialization.TypedStreamDeserializer;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;

/**
 * The {@link StreamSerializer} that handles:
 * <ol>
 * <li>{@link DataSerializable}</li>
 * <li>{@link IdentifiedDataSerializable}</li>
 * </ol>
 */
@SuppressWarnings("checkstyle:npathcomplexity")
final class DataSerializableSerializer implements StreamSerializer<DataSerializable>, TypedStreamDeserializer<DataSerializable> {

    public static final byte IDS_FLAG = 1 << 0;
    public static final byte EE_FLAG = 1 << 1;

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private final Version version = Version.of(BuildInfoProvider.getBuildInfo().getVersion());
    private final Int2ObjectHashMap<DataSerializableFactory> factories = new Int2ObjectHashMap<>();

    DataSerializableSerializer(Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                               ClassLoader classLoader) {
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

    @Override
    public int getTypeId() {
        return CONSTANT_TYPE_DATA_SERIALIZABLE;
    }

    @Override
    public DataSerializable read(ObjectDataInput in) throws IOException {
        return readInternal(in, null);
    }

    @Override
    public DataSerializable read(ObjectDataInput in, Class aClass)
            throws IOException {
        return readInternal(in, aClass);
    }

    private DataSerializable readInternal(ObjectDataInput in, Class aClass)
            throws IOException {
        setInputVersion(in, version);
        DataSerializable ds = null;
        if (null != aClass) {
            try {
                ds = (DataSerializable) aClass.newInstance();
            } catch (Exception e) {
                e = tryClarifyInstantiationException(aClass, e);
                throw new HazelcastSerializationException("Requested class " + aClass + " could not be instantiated.", e);
            }
        }

        final byte header = in.readByte();
        int id = 0;
        int factoryId = 0;
        String className = null;
        try {
            // If you ever change the way this is serialized think about to change
            // BasicOperationService::extractOperationCallId
            if (isFlagSet(header, IDS_FLAG)) {
                factoryId = in.readInt();
                final DataSerializableFactory dsf = factories.get(factoryId);
                if (dsf == null) {
                    throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
                }
                id = in.readInt();
                if (null == aClass) {
                    ds = dsf.create(id);
                    if (ds == null) {
                        throw new HazelcastSerializationException(dsf
                                + " is not be able to create an instance for ID: " + id + " on factory ID: " + factoryId);
                    }
                }
            } else {
                className = in.readString();
                if (null == aClass) {
                    ds = ClassLoaderUtil.newInstance(in.getClassLoader(), className);
                }
            }
            if (isFlagSet(header, EE_FLAG)) {
                in.readByte();
                in.readByte();
            }

            ds.readData(in);
            return ds;
        } catch (Exception e) {
            e = tryClarifyNoSuchMethodException(in.getClassLoader(), className, e);
            throw rethrowReadException(id, factoryId, className, e);
        }
    }

    public static boolean isFlagSet(byte value, byte flag) {
        return (value & flag) != 0;
    }

    private IOException rethrowReadException(int id, int factoryId, String className, Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException("Problem while reading DataSerializable, namespace: "
                + factoryId
                + ", ID: " + id
                + ", class: '" + className + "'"
                + ", exception: " + e.getMessage(), e);
    }

    private Exception tryClarifyInstantiationException(Class aClass, Exception exception) {
        if (!(exception instanceof InstantiationException)) {
            return exception;
        }
        InstantiationException instantiationException = (InstantiationException) exception;

        String message = tryGenerateClarifiedExceptionMessage(aClass);
        if (message == null) {
            return instantiationException;
        }

        InstantiationException clarifiedException = new InstantiationException(message);
        clarifiedException.initCause(instantiationException);
        return clarifiedException;
    }

    private Exception tryClarifyNoSuchMethodException(ClassLoader classLoader, String className, Exception exception) {
        if (!(exception instanceof NoSuchMethodException)) {
            return exception;
        }
        NoSuchMethodException noSuchMethodException = (NoSuchMethodException) exception;

        Class aClass;
        try {
            ClassLoader effectiveClassLoader = classLoader == null ? ClassLoaderUtil.class.getClassLoader() : classLoader;
            aClass = ClassLoaderUtil.loadClass(effectiveClassLoader, className);
        } catch (Exception e) {
            return noSuchMethodException;
        }

        String message = tryGenerateClarifiedExceptionMessage(aClass);
        if (message == null) {
            message = "Classes conforming to DataSerializable should provide a no-arguments constructor.";
        }

        NoSuchMethodException clarifiedException = new NoSuchMethodException(message);
        clarifiedException.initCause(noSuchMethodException);
        return clarifiedException;
    }

    @Override
    public void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        // If you ever change the way this is serialized think about to change
        // BasicOperationService::extractOperationCallId
        setOutputVersion(out, version);
        final boolean identified = obj instanceof IdentifiedDataSerializable;
        out.writeBoolean(identified);
        if (identified) {
            final IdentifiedDataSerializable ds = (IdentifiedDataSerializable) obj;
            out.writeInt(ds.getFactoryId());
            out.writeInt(ds.getClassId());
        } else {
            if (obj instanceof TypedDataSerializable) {
                out.writeString(((TypedDataSerializable) obj).getClassType().getName());
            } else {
                out.writeString(obj.getClass().getName());
            }
        }
        obj.writeData(out);
    }

    @Override
    public void destroy() {
        factories.clear();
    }

    private static void setOutputVersion(ObjectDataOutput out, Version version) {
        ((VersionedObjectDataOutput) out).setVersion(version);
    }

    private static void setInputVersion(ObjectDataInput in, Version version) {
        ((VersionedObjectDataInput) in).setVersion(version);
    }

    private static String tryGenerateClarifiedExceptionMessage(Class aClass) {
        String classType;
        if (aClass.isAnonymousClass()) {
            classType = "Anonymous";
        } else if (aClass.isLocalClass()) {
            classType = "Local";
        } else if (aClass.isMemberClass() && !Modifier.isStatic(aClass.getModifiers())) {
            classType = "Non-static member";
        } else {
            return null;
        }

        return String.format("%s classes can't conform to DataSerializable since they can't "
                + "provide an explicit no-arguments constructor.", classType);
    }

}
