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

import com.hazelcast.impl.map.BackupResponse;
import com.hazelcast.impl.map.GenericBackupOperation;
import com.hazelcast.impl.map.GetOperation;
import com.hazelcast.impl.map.PutOperation;
import com.hazelcast.impl.spi.Response;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.FastDataInputStream;
import com.hazelcast.nio.FastDataOutputStream;
import com.hazelcast.nio.HazelcastSerializationException;

import java.util.HashMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.nio.serialization.SerializationConstants.SERIALIZER_TYPE_DATA;

/**
 * @mdogan 6/19/12
 */
public final class DataSerializer implements TypeSerializer<DataSerializable> {

    private final static HashMap<String, DataSerializableFactory> factories = new HashMap<String, DataSerializableFactory>(100);

    static {
        factories.put(PutOperation.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new PutOperation();
            }
        });
        factories.put(Response.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new Response();
            }
        });
        factories.put(BackupResponse.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new BackupResponse();
            }
        });
        factories.put(GenericBackupOperation.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new GenericBackupOperation();
            }
        });
        factories.put(GetOperation.class.getName(), new DataSerializableFactory() {
            public DataSerializable create() {
                return new GetOperation();
            }
        });
    }

    public int getTypeId() {
        return SERIALIZER_TYPE_DATA;
    }

    interface DataSerializableFactory {
        DataSerializable create();
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
                    + className + ", exception: " + e);
        }
    }

    public final void write(final FastDataOutputStream out, final DataSerializable obj) throws Exception {
        out.writeUTF(obj.getClass().getName());
        obj.writeData(out);
    }

    public void destroy() {
    }
}
