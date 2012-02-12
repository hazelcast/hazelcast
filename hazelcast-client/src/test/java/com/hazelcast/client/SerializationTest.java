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

package com.hazelcast.client;

import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.FactoryImpl.ProxyKey;
import com.hazelcast.nio.DataSerializable;
import org.junit.Test;

import java.io.*;
import java.util.Date;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

public class SerializationTest {

    @Test(expected = RuntimeException.class)
    public void newNotSerializableException() {
        final Object o = new Object();
        IOUtil.toByte(o);
    }

    @Test
    public void newNullSerializer() {
        final Object o = null;
        final byte[] data = IOUtil.toByte(o);
        assertEquals(o, IOUtil.toObject(data));
    }

    @Test
    public void newStringSerializer() {
        final String s = "newStringSerializer 2@Z";
        final byte[] data = IOUtil.toByte(s);
        assertEquals(s, IOUtil.toObject(data));
    }

    @Test
    public void newDateSerializer() {
        final Date date = new Date();
        final byte[] data = IOUtil.toByte(date);
        assertEquals(date, IOUtil.toObject(data));
    }

    @Test
    public void newSerializerExternalizable() {
        final ExternalizableImpl o = new ExternalizableImpl();
        o.s = "Gallaxy";
        o.v = 42;
        final byte[] data = IOUtil.toByte(o);
        assertFalse(data.length == 0);
        assertFalse(o.readExternal);
        assertTrue(o.writeExternal);
        final ExternalizableImpl object = (ExternalizableImpl) IOUtil.toObject(data);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
        assertTrue(object.readExternal);
        assertFalse(object.writeExternal);
    }

    @Test
    public void newSerializerProxyKey() {
        final FactoryImpl.ProxyKey o = new ProxyKey("key", 15L);
        final byte[] data = IOUtil.toByte(o);
        assertFalse(data.length == 0);
        final ProxyKey object = (ProxyKey) IOUtil.toObject(data);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
    }

    private static class ExternalizableImpl implements Externalizable {
        private int v;
        private String s;

        private boolean readExternal = false;
        private boolean writeExternal = false;

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof ExternalizableImpl)) return false;
            final ExternalizableImpl other = (ExternalizableImpl) obj;
            return this.v == other.v &&
                    ((this.s == null && other.s == null) ||
                            (this.s != null && this.s.equals(other.s)));
        }

        @Override
        public int hashCode() {
            return this.v + 31 * (s != null ? s.hashCode() : 0);
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            v = in.readInt();
            s = in.readUTF();
            readExternal = true;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(v);
            out.writeUTF(s);
            writeExternal = true;
        }
    }

    private static class DataSerializableImpl implements DataSerializable {
        private int v;
        private String s;

        private boolean readExternal = false;
        private boolean writeExternal = false;

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof DataSerializableImpl)) return false;
            final DataSerializableImpl other = (DataSerializableImpl) obj;
            return this.v == other.v &&
                    ((this.s == null && other.s == null) ||
                            (this.s != null && this.s.equals(other.s)));
        }

        @Override
        public int hashCode() {
            return this.v + 31 * (s != null ? s.hashCode() : 0);
        }

        public void readData(DataInput in) throws IOException {
            v = in.readInt();
            s = in.readUTF();
            readExternal = true;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(v);
            out.writeUTF(s);
            writeExternal = true;
        }
    }
}
