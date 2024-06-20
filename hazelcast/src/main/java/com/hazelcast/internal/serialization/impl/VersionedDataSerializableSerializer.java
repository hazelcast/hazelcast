/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.TypedDataSerializable;
import com.hazelcast.nio.serialization.TypedStreamDeserializer;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.nio.serialization.impl.VersionedIdentifiedDataSerializable;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.DataSerializableHeader.createHeader;
import static com.hazelcast.internal.serialization.impl.DataSerializableHeader.isIdentifiedDataSerializable;
import static com.hazelcast.internal.serialization.impl.DataSerializableHeader.isVersioned;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;

/**
 * The {@link StreamSerializer} that handles:
 * <ol>
 * <li>{@link DataSerializable}</li>
 * <li>{@link IdentifiedDataSerializable}</li>
 * </ol>
 */
@SuppressWarnings("checkstyle:npathcomplexity")
class VersionedDataSerializableSerializer implements StreamSerializer<DataSerializable>,
        TypedStreamDeserializer<DataSerializable> {

    private static final String FACTORY_ID = "com.hazelcast.DataSerializerHook";

    private final ClusterVersionAware clusterVersionAware;

    private final Int2ObjectHashMap<DataSerializableFactory> factories = new Int2ObjectHashMap<>();

    VersionedDataSerializableSerializer(Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                        ClassLoader classLoader,
                                        ClusterVersionAware clusterVersionAware) {
        this(dataSerializableFactories, classLoader, clusterVersionAware, FACTORY_ID);
    }

    VersionedDataSerializableSerializer(Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                        ClassLoader classLoader,
                                        ClusterVersionAware clusterVersionAware,
                                        String factoryId) {
        this.clusterVersionAware = clusterVersionAware;
        try {
            List<DataSerializerHook> hooks = new ArrayList<>();
            ServiceLoader.iterator(DataSerializerHook.class, factoryId, classLoader)
                    .forEachRemaining(hooks::add);

            for (DataSerializerHook hook : hooks) {
                final DataSerializableFactory factory = hook.createFactory();
                if (factory != null) {
                    register(hook.getFactoryId(), factory);
                }
            }

            for (DataSerializerHook hook : hooks) {
                hook.afterFactoriesCreated(factories);
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
    public void destroy() {
        factories.clear();
    }

    @Override
    public void write(ObjectDataOutput out, DataSerializable obj) throws IOException {
        // if versions were previously set while processing an outer object,
        // keep the current values and restore them in the end
        Version previousClusterVersion = out.getVersion();

        if (out.getWanProtocolVersion() == Version.UNKNOWN) {
            // if WAN protocol version has not been yet explicitly set, then
            // check if class is Versioned to set cluster version as output version
            Version version = (obj instanceof Versioned)
                    ? clusterVersionAware.getClusterVersion()
                    : Version.UNKNOWN;
            out.setVersion(version);
        }

        if (obj instanceof IdentifiedDataSerializable serializable) {
            writeIdentifiedDataSerializable(out, serializable, out.getVersion(), out.getWanProtocolVersion());
        } else {
            writeDataSerializable(out, obj, out.getVersion(), out.getWanProtocolVersion());
        }
        obj.writeData(out);

        // restore the original version
        if (out.getWanProtocolVersion() == Version.UNKNOWN) {
            out.setVersion(previousClusterVersion);
        }
    }

    private void writeIdentifiedDataSerializable(ObjectDataOutput out,
                                                 IdentifiedDataSerializable obj,
                                                 Version clusterVersion,
                                                 Version wanProtocolVersion) throws IOException {

        boolean versioned = clusterVersion != Version.UNKNOWN || wanProtocolVersion != Version.UNKNOWN;

        out.writeByte(createHeader(true, versioned));

        out.writeInt(obj.getFactoryId());
        out.writeInt(obj instanceof VersionedIdentifiedDataSerializable vids
                ? vids.getClassId(clusterVersion)
                : obj.getClassId());

        if (wanProtocolVersion != Version.UNKNOWN) {
            // we write out WAN protocol versions as negative major version
            out.writeByte(-wanProtocolVersion.getMajor());
            out.writeByte(wanProtocolVersion.getMinor());
        } else if (clusterVersion != Version.UNKNOWN) {
            out.writeByte(clusterVersion.getMajor());
            out.writeByte(clusterVersion.getMinor());
        }
    }

    private void writeDataSerializable(ObjectDataOutput out,
                                       DataSerializable obj,
                                       Version clusterVersion,
                                       Version wanProtocolVersion) throws IOException {
        boolean versioned = clusterVersion != Version.UNKNOWN || wanProtocolVersion != Version.UNKNOWN;
        out.writeByte(createHeader(false, versioned));

        if (obj instanceof TypedDataSerializable serializable) {
            out.writeString(serializable.getClassType().getName());
        } else {
            out.writeString(obj.getClass().getName());
        }

        if (wanProtocolVersion != Version.UNKNOWN) {
            // we write out WAN protocol versions as negative major version
            out.writeByte(-wanProtocolVersion.getMajor());
            out.writeByte(wanProtocolVersion.getMinor());
        } else if (clusterVersion != Version.UNKNOWN) {
            out.writeByte(clusterVersion.getMajor());
            out.writeByte(clusterVersion.getMinor());
        }
    }


    @Override
    public DataSerializable read(ObjectDataInput in, Class clazz) throws IOException {
        DataSerializable instance = null;
        if (null != clazz) {
            try {
                instance = (DataSerializable) clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new HazelcastSerializationException("Requested class " + clazz + " could not be instantiated.", e);
            }
        }
        return doRead(in, instance);
    }

    @Override
    public DataSerializable read(ObjectDataInput in) throws IOException {
        return doRead(in, null);
    }

    private DataSerializable doRead(ObjectDataInput in, DataSerializable instance) throws IOException {
        byte header = in.readByte();
        if (isIdentifiedDataSerializable(header)) {
            return readIdentifiedDataSerializable(in, header, instance);
        } else {
            return readDataSerializable(in, header, instance);
        }
    }

    private DataSerializable readIdentifiedDataSerializable(ObjectDataInput in, byte header, DataSerializable instance)
            throws IOException {
        int factoryId = 0;
        int classId = 0;
        try {
            // read factoryId & classId
            factoryId = in.readInt();
            classId = in.readInt();

            // if versions were previously set while processing an outer object,
            // keep the current values and restore them in the end
            Version previousClusterVersion = in.getVersion();
            Version previousWanProtocolVersion = in.getWanProtocolVersion();

            if (isVersioned(header)) {
                readVersions(in);
            } else {
                in.setVersion(Version.UNKNOWN);
                in.setWanProtocolVersion(Version.UNKNOWN);
            }

            // populate the object
            DataSerializable ds = instance != null
                    ? instance
                    : createIdentifiedDataSerializable(in.getVersion(), in.getWanProtocolVersion(), factoryId, classId);
            ds.readData(in);

            // restore the original versions
            in.setVersion(previousClusterVersion);
            in.setWanProtocolVersion(previousWanProtocolVersion);
            return ds;
        } catch (Exception ex) {
            throw rethrowIdsReadException(factoryId, classId, ex);
        }
    }

    /**
     * Reads the cluster and WAN protocol version from the serialised stream and
     * sets them on the {@code in} object.
     *
     * @param in the input stream containing the cluster or WAN protocol version
     * @throws IOException if an I/O error occurs.
     */
    private void readVersions(ObjectDataInput in) throws IOException {
        byte major = in.readByte();
        byte minor = in.readByte();
        assert clusterVersionAware.getClusterVersion() != null;
        if (major < 0) {
            in.setWanProtocolVersion(Version.of(-major, minor));
        } else {
            in.setVersion(Version.of(major, minor));
        }
    }

    private DataSerializable createIdentifiedDataSerializable(Version clusterVersion,
                                                              Version wanProtocolVersion,
                                                              int factoryId,
                                                              int classId) {
        DataSerializableFactory dsf = factories.get(factoryId);
        if (dsf == null) {
            throw new HazelcastSerializationException("No DataSerializerFactory registered for namespace: " + factoryId);
        } else {
            return dsf.create(classId);
        }
    }

    private DataSerializable readDataSerializable(ObjectDataInput in, byte header, DataSerializable instance) throws IOException {
        String className = in.readString();
        try {
            // if versions were previously set while processing an outer object,
            // keep the current values and restore them in the end
            Version previousClusterVersion = in.getVersion();
            Version previousWanProtocolVersion = in.getWanProtocolVersion();

            if (isVersioned(header)) {
                readVersions(in);
            } else {
                in.setVersion(Version.UNKNOWN);
                in.setWanProtocolVersion(Version.UNKNOWN);
            }

            DataSerializable ds = instance != null ? instance
                    : ClassLoaderUtil.newInstance(in.getClassLoader(), className);
            ds.readData(in);

            // restore the original versions
            in.setVersion(previousClusterVersion);
            in.setWanProtocolVersion(previousWanProtocolVersion);
            return ds;
        } catch (Exception ex) {
            throw rethrowDsReadException(className, ex);
        }
    }



    private static IOException rethrowIdsReadException(int factoryId, int classId, Exception e) throws IOException {
        if (e instanceof IOException exception) {
            throw exception;
        }
        if (e instanceof HazelcastSerializationException exception) {
            throw exception;
        }
        throw new HazelcastSerializationException("Problem while reading IdentifiedDataSerializable, namespace: " + factoryId
                                                  + ", classId: " + classId
                                                  + ", exception: " + e.getMessage(), e);
    }

    private static IOException rethrowDsReadException(String className, Exception e) throws IOException {
        if (e instanceof IOException exception) {
            throw exception;
        }
        if (e instanceof HazelcastSerializationException exception) {
            throw exception;
        }
        throw new HazelcastSerializationException("Problem while reading DataSerializable, class-name: " + className
                                                  + ", exception: " + e.getMessage(), e);
    }


}
