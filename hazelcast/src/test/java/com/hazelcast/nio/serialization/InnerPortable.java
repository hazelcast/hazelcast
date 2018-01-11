/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Arrays;

class InnerPortable implements Portable {

    byte[] bb;
    char[] cc;
    short[] ss;
    int[] ii;
    long[] ll;
    float[] ff;
    double[] dd;
    NamedPortable[] nn;

    InnerPortable() {
    }

    InnerPortable(byte[] bb, char[] cc, short[] ss, int[] ii, long[] ll, float[] ff, double[] dd, NamedPortable[] nn) {
        this.bb = bb;
        this.cc = cc;
        this.ss = ss;
        this.ii = ii;
        this.ll = ll;
        this.ff = ff;
        this.dd = dd;
        this.nn = nn;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.INNER_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeByteArray("b", bb);
        writer.writeCharArray("c", cc);
        writer.writeShortArray("s", ss);
        writer.writeIntArray("i", ii);
        writer.writeLongArray("l", ll);
        writer.writeFloatArray("f", ff);
        writer.writeDoubleArray("d", dd);
        writer.writePortableArray("nn", nn);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        bb = reader.readByteArray("b");
        cc = reader.readCharArray("c");
        ss = reader.readShortArray("s");
        ii = reader.readIntArray("i");
        ll = reader.readLongArray("l");
        ff = reader.readFloatArray("f");
        dd = reader.readDoubleArray("d");
        Portable[] pp = reader.readPortableArray("nn");
        nn = new NamedPortable[pp.length];
        System.arraycopy(pp, 0, nn, 0, nn.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InnerPortable that = (InnerPortable) o;
        if (!Arrays.equals(bb, that.bb)) {
            return false;
        }
        if (!Arrays.equals(cc, that.cc)) {
            return false;
        }
        if (!Arrays.equals(dd, that.dd)) {
            return false;
        }
        if (!Arrays.equals(ff, that.ff)) {
            return false;
        }
        if (!Arrays.equals(ii, that.ii)) {
            return false;
        }
        if (!Arrays.equals(ll, that.ll)) {
            return false;
        }
        if (!Arrays.equals(nn, that.nn)) {
            return false;
        }
        if (!Arrays.equals(ss, that.ss)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = bb != null ? Arrays.hashCode(bb) : 0;
        result = 31 * result + (cc != null ? Arrays.hashCode(cc) : 0);
        result = 31 * result + (ss != null ? Arrays.hashCode(ss) : 0);
        result = 31 * result + (ii != null ? Arrays.hashCode(ii) : 0);
        result = 31 * result + (ll != null ? Arrays.hashCode(ll) : 0);
        result = 31 * result + (ff != null ? Arrays.hashCode(ff) : 0);
        result = 31 * result + (dd != null ? Arrays.hashCode(dd) : 0);
        result = 31 * result + (nn != null ? Arrays.hashCode(nn) : 0);
        return result;
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InnerPortable{");
        sb.append("bb=").append(Arrays.toString(bb));
        sb.append(", cc=").append(Arrays.toString(cc));
        sb.append(", ss=").append(Arrays.toString(ss));
        sb.append(", ii=").append(Arrays.toString(ii));
        sb.append(", ll=").append(Arrays.toString(ll));
        sb.append(", ff=").append(Arrays.toString(ff));
        sb.append(", dd=").append(Arrays.toString(dd));
        sb.append(", nn=").append(Arrays.toString(nn));
        sb.append('}');
        return sb.toString();
    }
}
