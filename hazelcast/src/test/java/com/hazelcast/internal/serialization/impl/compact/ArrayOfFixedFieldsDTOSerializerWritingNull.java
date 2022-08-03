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
package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;

public class ArrayOfFixedFieldsDTOSerializerWritingNull implements CompactSerializer<ArrayOfFixedFieldsDTO> {
    @Nonnull
    @Override
    public ArrayOfFixedFieldsDTO read(@Nonnull CompactReader reader) {
        byte[] b = reader.readArrayOfInt8("b");
        boolean[] bool = reader.readArrayOfBoolean("bool");
        short[] s = reader.readArrayOfInt16("s");
        int[] i = reader.readArrayOfInt32("i");
        long[] l = reader.readArrayOfInt64("l");
        float[] f = reader.readArrayOfFloat32("f");
        double[] d = reader.readArrayOfFloat64("d");
        return new ArrayOfFixedFieldsDTO(b, bool, s, i, l, f, d);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull ArrayOfFixedFieldsDTO object) {
        Byte[] bb = new Byte[object.b.length];
        Boolean[] bools = new Boolean[object.bool.length];
        Short[] ss = new Short[object.s.length];
        Integer[] ii = new Integer[object.i.length];
        Long[] ll = new Long[object.l.length];
        Float[] ff = new Float[object.f.length];
        Double[] dd = new Double[object.d.length];
        out.writeArrayOfNullableInt8("b", bb);
        out.writeArrayOfNullableBoolean("bool", bools);
        out.writeArrayOfNullableInt16("s", ss);
        out.writeArrayOfNullableInt32("i", ii);
        out.writeArrayOfNullableInt64("l", ll);
        out.writeArrayOfNullableFloat32("f", ff);
        out.writeArrayOfNullableFloat64("d", dd);
    }
}
