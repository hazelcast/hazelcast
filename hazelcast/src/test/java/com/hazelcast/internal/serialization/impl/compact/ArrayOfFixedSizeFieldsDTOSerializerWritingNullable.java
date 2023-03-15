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

public class ArrayOfFixedSizeFieldsDTOSerializerWritingNullable implements CompactSerializer<ArrayOfFixedSizeFieldsDTO> {
    @Nonnull
    @Override
    public ArrayOfFixedSizeFieldsDTO read(@Nonnull CompactReader reader) {
        byte[] b = reader.readArrayOfInt8("b");
        boolean[] bool = reader.readArrayOfBoolean("bool");
        short[] s = reader.readArrayOfInt16("s");
        int[] i = reader.readArrayOfInt32("i");
        long[] l = reader.readArrayOfInt64("l");
        float[] f = reader.readArrayOfFloat32("f");
        double[] d = reader.readArrayOfFloat64("d");
        return new ArrayOfFixedSizeFieldsDTO(b, bool, s, i, l, f, d);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull ArrayOfFixedSizeFieldsDTO object) {
        if (object.b == null) {
            out.writeArrayOfNullableInt8("b", null);
        } else {
            Byte[] bb = new Byte[object.b.length];
            for (int k = 0; k < object.b.length; k++) {
                bb[k] = object.b[k];
            }
            out.writeArrayOfNullableInt8("b", bb);
        }
        if (object.bool == null) {
            out.writeArrayOfNullableBoolean("bool", null);
        } else {
            Boolean[] bools = new Boolean[object.bool.length];
            for (int k = 0; k < object.bool.length; k++) {
                bools[k] = object.bool[k];
            }
            out.writeArrayOfNullableBoolean("bool", bools);
        }
        if (object.s == null) {
            out.writeArrayOfNullableInt16("s", null);
        } else {
            Short[] ss = new Short[object.s.length];
            for (int k = 0; k < object.s.length; k++) {
                ss[k] = object.s[k];
            }
            out.writeArrayOfNullableInt16("s", ss);
        }
        if (object.i == null) {
            out.writeArrayOfNullableInt32("i", null);
        } else {
            Integer[] ii = new Integer[object.i.length];
            for (int k = 0; k < object.i.length; k++) {
                ii[k] = object.i[k];
            }
            out.writeArrayOfNullableInt32("i", ii);
        }
        if (object.l == null) {
            out.writeArrayOfNullableInt64("l", null);
        } else {
            Long[] ll = new Long[object.l.length];
            for (int k = 0; k < object.l.length; k++) {
                ll[k] = object.l[k];
            }
            out.writeArrayOfNullableInt64("l", ll);
        }
        if (object.f == null) {
            out.writeArrayOfNullableFloat32("f", null);
        } else {
            Float[] ff = new Float[object.f.length];
            for (int k = 0; k < object.f.length; k++) {
                ff[k] = object.f[k];
            }
            out.writeArrayOfNullableFloat32("f", ff);
        }
        if (object.d == null) {
            out.writeArrayOfNullableFloat64("d", null);
        } else {
            Double[] dd = new Double[object.d.length];
            for (int k = 0; k < object.d.length; k++) {
                dd[k] = object.d[k];
            }
            out.writeArrayOfNullableFloat64("d", dd);
        }
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
