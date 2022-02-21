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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Arrays;

class RawDataPortable implements Portable {

    long l;
    char[] c;
    NamedPortable p;
    int k;
    String s;
    ByteArrayDataSerializable sds;

    RawDataPortable() {
    }

    RawDataPortable(long l, char[] c, NamedPortable p, int k, String s, ByteArrayDataSerializable sds) {
        this.l = l;
        this.c = c;
        this.p = p;
        this.k = k;
        this.s = s;
        this.sds = sds;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.RAW_DATA_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("l", l);
        writer.writeCharArray("c", c);
        writer.writePortable("p", p);
        final ObjectDataOutput output = writer.getRawDataOutput();
        output.writeInt(k);
        output.writeString(s);
        output.writeObject(sds);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        l = reader.readLong("l");
        c = reader.readCharArray("c");
        p = reader.readPortable("p");
        final ObjectDataInput input = reader.getRawDataInput();
        k = input.readInt();
        s = input.readString();
        sds = input.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RawDataPortable that = (RawDataPortable) o;

        if (k != that.k) {
            return false;
        }
        if (l != that.l) {
            return false;
        }
        if (!Arrays.equals(c, that.c)) {
            return false;
        }
        if (p != null ? !p.equals(that.p) : that.p != null) {
            return false;
        }
        if (s != null ? !s.equals(that.s) : that.s != null) {
            return false;
        }
        if (sds != null ? !sds.equals(that.sds) : that.sds != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (l ^ (l >>> 32));
        result = 31 * result + (c != null ? Arrays.hashCode(c) : 0);
        result = 31 * result + (p != null ? p.hashCode() : 0);
        result = 31 * result + k;
        result = 31 * result + (s != null ? s.hashCode() : 0);
        result = 31 * result + (sds != null ? sds.hashCode() : 0);
        return result;
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }
}
