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

public class InnerDTOSerializerReadingWrongTypedField implements CompactSerializer<InnerDTO> {
    @Nonnull
    @Override
    public InnerDTO read(@Nonnull CompactReader in) {
        // Every reader method tries to read the previous field.
        boolean[] bools = in.readArrayOfBoolean("nullableDoubles", new boolean[0]);
        byte[] bytes = in.readArrayOfInt8("bools", new byte[0]);
        // array of char and array of long reader methods are switched to make them read the wrong type
        // due to char array and short array using the same reader method
        short[] shorts = in.readArrayOfInt16("bytes", new short[0]);
        // this would be 'shorts', but switched with 'ints'
        char[] chars = CompactUtil.charArrayFromShortArray(in.readArrayOfInt16("ints", new short[0]));
        int[] ints = in.readArrayOfInt32("chars", new int[0]);
        // this would be 'ints', but switched with 'shorts'
        long[] longs = in.readArrayOfInt64("shorts", new long[0]);
        float[] floats = in.readArrayOfFloat32("longs", new float[0]);
        double[] doubles = in.readArrayOfFloat64("floats", new double[0]);
        String[] strings = in.readArrayOfString("doubles", new String[0]);
        NamedDTO[] namedDTOS = in.readArrayOfCompact("strings", NamedDTO.class, new NamedDTO[0]);
        BigDecimal[] bigDecimals = in.readArrayOfDecimal("nn", new BigDecimal[0]);
        LocalTime[] localTimes = in.readArrayOfTime("bigDecimals", new LocalTime[0]);
        LocalDate[] localDates = in.readArrayOfDate("localTimes", new LocalDate[0]);
        LocalDateTime[] localDateTimes = in.readArrayOfTimestamp("localDates", new LocalDateTime[0]);
        OffsetDateTime[] offsetDateTimes = in.readArrayOfTimestampWithTimezone("localDateTimes", new OffsetDateTime[0]);
        Boolean[] nullableBools = in.readArrayOfNullableBoolean("offsetDateTimes", new Boolean[0]);
        Byte[] nullableBytes = in.readArrayOfNullableInt8("nullableBools", new Byte[0]);
        // array of nullable short and array of nullable long reader methods are switched to make them read
        // the wrong type due to nullable char array and nullable short array using the same reader method
        Short[] nullableCharactersAsShorts = in.readArrayOfNullableInt16("nullableBytes", new Short[0]);
        Character[] nullableCharacters = CompactUtil.characterArrayFromShortArray(nullableCharactersAsShorts);
        // this would be 'nullableCharacters', but switched with 'nullableIntegers'
        Short[] nullableShorts = in.readArrayOfNullableInt16("nullableIntegers", new Short[0]);
        Integer[] nullableIntegers = in.readArrayOfNullableInt32("nullableShorts", new Integer[0]);
        // this would be 'nullableIntegers', but switched with 'nullableCharacters'
        Long[] nullableLongs = in.readArrayOfNullableInt64("nullableCharacters", new Long[0]);
        Float[] nullableFloats = in.readArrayOfNullableFloat32("nullableLongs", new Float[0]);
        Double[] nullableDoubles = in.readArrayOfNullableFloat64("nullableFloats", new Double[0]);
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
