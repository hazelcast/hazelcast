/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

class MainPortable implements Portable {

    byte b;
    boolean bool;
    char c;
    short s;
    int i;
    long l;
    float f;
    double d;
    String str;
    InnerPortable p;

    MainPortable() {
    }

    MainPortable(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str, InnerPortable p) {
        this.b = b;
        this.bool = bool;
        this.c = c;
        this.s = s;
        this.i = i;
        this.l = l;
        this.f = f;
        this.d = d;
        this.str = str;
        this.p = p;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.MAIN_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeByte("b", b);
        writer.writeBoolean("bool", bool);
        writer.writeChar("c", c);
        writer.writeShort("s", s);
        writer.writeInt("i", i);
        writer.writeLong("l", l);
        writer.writeFloat("f", f);
        writer.writeDouble("d", d);
        writer.writeUTF("str", str);
        if (p != null) {
            writer.writePortable("p", p);
        } else {
            writer.writeNullPortable("p", TestSerializationConstants.PORTABLE_FACTORY_ID,
                    TestSerializationConstants.INNER_PORTABLE);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        b = reader.readByte("b");
        bool = reader.readBoolean("bool");
        c = reader.readChar("c");
        s = reader.readShort("s");
        i = reader.readInt("i");
        l = reader.readLong("l");
        f = reader.readFloat("f");
        d = reader.readDouble("d");
        str = reader.readUTF("str");
        p = reader.readPortable("p");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MainPortable that = (MainPortable) o;
        if (b != that.b) {
            return false;
        }
        if (bool != that.bool) {
            return false;
        }
        if (c != that.c) {
            return false;
        }
        if (Double.compare(that.d, d) != 0) {
            return false;
        }
        if (Float.compare(that.f, f) != 0) {
            return false;
        }
        if (i != that.i) {
            return false;
        }
        if (l != that.l) {
            return false;
        }
        if (s != that.s) {
            return false;
        }
        if (p != null ? !p.equals(that.p) : that.p != null) {
            return false;
        }
        if (str != null ? !str.equals(that.str) : that.str != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) b;
        result = 31 * result + (bool ? 1 : 0);
        result = 31 * result + (int) c;
        result = 31 * result + (int) s;
        result = 31 * result + i;
        result = 31 * result + (int) (l ^ (l >>> 32));
        result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
        temp = d != +0.0d ? Double.doubleToLongBits(d) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (str != null ? str.hashCode() : 0);
        result = 31 * result + (p != null ? p.hashCode() : 0);
        return result;
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }
}
