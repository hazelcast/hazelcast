/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class ArrayOfFixedSizeFieldsDTOSerializerReadingNullable implements CompactSerializer<ArrayOfFixedSizeFieldsDTO> {
    @Nonnull
    @Override
    public ArrayOfFixedSizeFieldsDTO read(@Nonnull CompactReader reader) {
        Byte[] b = reader.readArrayOfNullableInt8("b");
        Boolean[] bool = reader.readArrayOfNullableBoolean("bool");
        Short[] s = reader.readArrayOfNullableInt16("s");
        Integer[] i = reader.readArrayOfNullableInt32("i");
        Long[] l = reader.readArrayOfNullableInt64("l");
        Float[] f = reader.readArrayOfNullableFloat32("f");
        Double[] d = reader.readArrayOfNullableFloat64("d");
        byte[] pBytes;
        if (b == null) {
            pBytes = null;
        } else {
            pBytes = new byte[b.length];
            for (int k = 0; k < b.length; k++) {
                pBytes[k] = b[k];
            }
        }
        boolean[] pBools;
        if (bool == null) {
            pBools = null;
        } else {
            pBools = new boolean[bool.length];
            for (int k = 0; k < bool.length; k++) {
                pBools[k] = bool[k];
            }
        }
        short[] pShorts;
        if (s == null) {
            pShorts = null;
        } else {
            pShorts = new short[s.length];
            for (int k = 0; k < s.length; k++) {
                pShorts[k] = s[k];
            }
        }
        int[] pInts;
        if (i == null) {
            pInts = null;
        } else {
            pInts = new int[i.length];
            for (int k = 0; k < i.length; k++) {
                pInts[k] = i[k];
            }
        }
        long[] pLongs;
        if (l == null) {
            pLongs = null;
        } else {
            pLongs = new long[l.length];
            for (int k = 0; k < l.length; k++) {
                pLongs[k] = l[k];
            }
        }
        float[] pFloats;
        if (f == null) {
            pFloats = null;
        } else {
            pFloats = new float[f.length];
            for (int k = 0; k < f.length; k++) {
                pFloats[k] = f[k];
            }
        }
        double[] pDoubles;
        if (d == null) {
            pDoubles = null;
        } else {
            pDoubles = new double[d.length];
            for (int k = 0; k < d.length; k++) {
                pDoubles[k] = d[k];
            }
        }
        return new ArrayOfFixedSizeFieldsDTO(pBytes, pBools, pShorts, pInts, pLongs, pFloats, pDoubles);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull ArrayOfFixedSizeFieldsDTO object) {
        out.writeArrayOfInt8("b", object.b);
        out.writeArrayOfBoolean("bool", object.bool);
        out.writeArrayOfInt16("s", object.s);
        out.writeArrayOfInt32("i", object.i);
        out.writeArrayOfInt64("l", object.l);
        out.writeArrayOfFloat32("f", object.f);
        out.writeArrayOfFloat64("d", object.d);
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "arrayOfFixedSizeFieldsDTO";
    }

    @Nonnull
    @Override
    public Class<ArrayOfFixedSizeFieldsDTO> getCompactClass() {
        return ArrayOfFixedSizeFieldsDTO.class;
    }
}
