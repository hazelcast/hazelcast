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
import example.serialization.InnerDTO;
import example.serialization.NamedDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class InnerDTOSerializerReadingMissingFields implements CompactSerializer<InnerDTO> {
    @Nonnull
    @Override
    public InnerDTO read(@Nonnull CompactReader in) {
        boolean[] bools = in.readArrayOfBoolean("nonExistingField", new boolean[0]);
        byte[] bytes = in.readArrayOfInt8("nonExistingField", new byte[0]);
        short[] shorts = in.readArrayOfInt16("nonExistingField", new short[0]);
        char[] chars = CompactUtil.charArrayFromShortArray(in.readArrayOfInt16("nonExistingField", new short[0]));
        int[] ints = in.readArrayOfInt32("nonExistingField", new int[0]);
        long[] longs = in.readArrayOfInt64("nonExistingField", new long[0]);
        float[] floats = in.readArrayOfFloat32("nonExistingField", new float[0]);
        double[] doubles = in.readArrayOfFloat64("nonExistingField", new double[0]);
        String[] strings = in.readArrayOfString("nonExistingField", new String[0]);
        NamedDTO[] namedDTOS = in.readArrayOfCompact("nonExistingField", NamedDTO.class, new NamedDTO[0]);
        BigDecimal[] bigDecimals = in.readArrayOfDecimal("nonExistingField", new BigDecimal[0]);
        LocalTime[] localTimes = in.readArrayOfTime("nonExistingField", new LocalTime[0]);
        LocalDate[] localDates = in.readArrayOfDate("nonExistingField", new LocalDate[0]);
        LocalDateTime[] localDateTimes = in.readArrayOfTimestamp("nonExistingField", new LocalDateTime[0]);
        OffsetDateTime[] offsetDateTimes = in.readArrayOfTimestampWithTimezone("nonExistingField", new OffsetDateTime[0]);
        Boolean[] nullableBools = in.readArrayOfNullableBoolean("nonExistingField", new Boolean[0]);
        Byte[] nullableBytes = in.readArrayOfNullableInt8("nonExistingField", new Byte[0]);
        Short[] nullableCharactersAsShorts = in.readArrayOfNullableInt16("nonExistingField", new Short[0]);
        Character[] nullableCharacters = CompactUtil.characterArrayFromShortArray(nullableCharactersAsShorts);
        Short[] nullableShorts = in.readArrayOfNullableInt16("nonExistingField", new Short[0]);
        Integer[] nullableIntegers = in.readArrayOfNullableInt32("nonExistingField", new Integer[0]);
        Long[] nullableLongs = in.readArrayOfNullableInt64("nonExistingField", new Long[0]);
        Float[] nullableFloats = in.readArrayOfNullableFloat32("nonExistingField", new Float[0]);
        Double[] nullableDoubles = in.readArrayOfNullableFloat64("nonExistingField", new Double[0]);
        return new InnerDTO(bools, bytes, chars, shorts, ints, longs, floats, doubles, strings, namedDTOS,
                bigDecimals, localTimes, localDates, localDateTimes, offsetDateTimes, nullableBools, nullableBytes,
                nullableCharacters, nullableShorts, nullableIntegers, nullableLongs, nullableFloats, nullableDoubles);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull InnerDTO object) {
        out.writeArrayOfBoolean("bools", object.bools);
        out.writeArrayOfInt8("bytes", object.bytes);
        out.writeArrayOfInt16("chars", CompactUtil.charArrayAsShortArray(object.chars));
        out.writeArrayOfInt16("shorts", object.shorts);
        out.writeArrayOfInt32("ints", object.ints);
        out.writeArrayOfInt64("longs", object.longs);
        out.writeArrayOfFloat32("floats", object.floats);
        out.writeArrayOfFloat64("doubles", object.doubles);
        out.writeArrayOfString("strings", object.strings);
        out.writeArrayOfCompact("nn", object.nn);
        out.writeArrayOfDecimal("bigDecimals", object.bigDecimals);
        out.writeArrayOfTime("localTimes", object.localTimes);
        out.writeArrayOfDate("localDates", object.localDates);
        out.writeArrayOfTimestamp("localDateTimes", object.localDateTimes);
        out.writeArrayOfTimestampWithTimezone("offsetDateTimes", object.offsetDateTimes);
        out.writeArrayOfNullableBoolean("nullableBools", object.nullableBools);
        out.writeArrayOfNullableInt8("nullableBytes", object.nullableBytes);
        out.writeArrayOfNullableInt16("nullableCharacters", CompactUtil.characterArrayAsShortArray(object.nullableCharacters));
        out.writeArrayOfNullableInt16("nullableShorts", object.nullableShorts);
        out.writeArrayOfNullableInt32("nullableIntegers", object.nullableIntegers);
        out.writeArrayOfNullableInt64("nullableLongs", object.nullableLongs);
        out.writeArrayOfNullableFloat32("nullableFloats", object.nullableFloats);
        out.writeArrayOfNullableFloat64("nullableDoubles", object.nullableDoubles);
    }
}
