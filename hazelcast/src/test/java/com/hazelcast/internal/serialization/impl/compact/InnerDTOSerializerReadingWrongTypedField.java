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

import com.hazelcast.nio.serialization.FieldKind;
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
        boolean[] bools;
        if (in.getFieldKind("nullableDoubles") == FieldKind.ARRAY_OF_BOOLEAN) {
            bools = in.readArrayOfBoolean("nullableDoubles");
        } else {
            bools = new boolean[0];
        }
        byte[] bytes;
        if (in.getFieldKind("bools") == FieldKind.ARRAY_OF_INT8) {
            bytes = in.readArrayOfInt8("bools");
        } else {
            bytes = new byte[0];
        }
        // array of char and array of long reader methods are switched to make them read the wrong type
        // due to char array and short array using the same reader method
        short[] shorts;
        if (in.getFieldKind("bytes") == FieldKind.ARRAY_OF_INT16) {
            shorts = in.readArrayOfInt16("bytes");
        } else {
            shorts = new short[0];
        }
        char[] chars;
        // this would be 'shorts', but switched with 'ints'
        if (in.getFieldKind("ints") == FieldKind.ARRAY_OF_INT16) {
            chars = CompactUtil.charArrayFromShortArray(in.readArrayOfInt16("ints"));
        } else {
            chars = CompactUtil.charArrayFromShortArray(new short[0]);
        }
        int[] ints;
        if (in.getFieldKind("chars") == FieldKind.ARRAY_OF_INT32) {
            ints = in.readArrayOfInt32("chars");
        } else {
            ints = new int[0];
        }
        long[] longs;
        // this would be 'ints', but switched with 'shorts'
        if (in.getFieldKind("shorts") == FieldKind.ARRAY_OF_INT64) {
            longs = in.readArrayOfInt64("shorts");
        } else {
            longs = new long[0];
        }
        float[] floats;
        if (in.getFieldKind("longs") == FieldKind.ARRAY_OF_FLOAT32) {
            floats = in.readArrayOfFloat32("longs");
        } else {
            floats = new float[0];
        }
        double[] doubles;
        if (in.getFieldKind("floats") == FieldKind.ARRAY_OF_FLOAT64) {
            doubles = in.readArrayOfFloat64("floats");
        } else {
            doubles = new double[0];
        }
        String[] strings;
        if (in.getFieldKind("doubles") == FieldKind.ARRAY_OF_STRING) {
            strings = in.readArrayOfString("doubles");
        } else {
            strings = new String[0];
        }
        NamedDTO[] namedDTOs;
        if (in.getFieldKind("strings") == FieldKind.ARRAY_OF_COMPACT) {
            namedDTOs = in.readArrayOfCompact("strings", NamedDTO.class);
        } else {
            namedDTOs = new NamedDTO[0];
        }
        BigDecimal[] bigDecimals;
        if (in.getFieldKind("nn") == FieldKind.ARRAY_OF_DECIMAL) {
            bigDecimals = in.readArrayOfDecimal("nn");
        } else {
            bigDecimals = new BigDecimal[0];
        }
        LocalTime[] localTimes;
        if (in.getFieldKind("bigDecimals") == FieldKind.ARRAY_OF_TIME) {
            localTimes = in.readArrayOfTime("bigDecimals");
        } else {
            localTimes = new LocalTime[0];
        }
        LocalDate[] localDates;
        if (in.getFieldKind("localTimes") == FieldKind.ARRAY_OF_DATE) {
            localDates = in.readArrayOfDate("localTimes");
        } else {
            localDates = new LocalDate[0];
        }
        LocalDateTime[] localDateTimes;
        if (in.getFieldKind("localDates") == FieldKind.ARRAY_OF_TIMESTAMP) {
            localDateTimes = in.readArrayOfTimestamp("localDates");
        } else {
            localDateTimes = new LocalDateTime[0];
        }
        OffsetDateTime[] offsetDateTimes;
        if (in.getFieldKind("localDateTimes") == FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE) {
            offsetDateTimes = in.readArrayOfTimestampWithTimezone("localDateTimes");
        } else {
            offsetDateTimes = new OffsetDateTime[0];
        }
        Boolean[] nullableBools;
        if (in.getFieldKind("offsetDateTimes") == FieldKind.ARRAY_OF_NULLABLE_BOOLEAN) {
            nullableBools = in.readArrayOfNullableBoolean("offsetDateTimes");
        } else {
            nullableBools = new Boolean[0];
        }
        Byte[] nullableBytes;
        if (in.getFieldKind("nullableBools") == FieldKind.ARRAY_OF_NULLABLE_INT8) {
            nullableBytes = in.readArrayOfNullableInt8("nullableBools");
        } else {
            nullableBytes = new Byte[0];
        }
        // array of nullable short and array of nullable long reader methods are switched to make them read
        // the wrong type due to nullable char array and nullable short array using the same reader method
        Short[] nullableCharactersAsShorts;
        if (in.getFieldKind("nullableBytes") == FieldKind.ARRAY_OF_NULLABLE_INT16) {
            nullableCharactersAsShorts = in.readArrayOfNullableInt16("nullableBytes");
        } else {
            nullableCharactersAsShorts = new Short[0];
        }
        Character[] nullableCharacters = CompactUtil.characterArrayFromShortArray(nullableCharactersAsShorts);
        Short[] nullableShorts;
        // this would be 'nullableCharacters', but switched with 'nullableIntegers'
        if (in.getFieldKind("nullableIntegers") == FieldKind.ARRAY_OF_NULLABLE_INT16) {
            nullableShorts = in.readArrayOfNullableInt16("nullableIntegers");
        } else {
            nullableShorts = new Short[0];
        }
        Integer[] nullableIntegers;
        if (in.getFieldKind("nullableShorts") == FieldKind.ARRAY_OF_NULLABLE_INT32) {
            nullableIntegers = in.readArrayOfNullableInt32("nullableShorts");
        } else {
            nullableIntegers = new Integer[0];
        }
        // this would be 'nullableIntegers', but switched with 'nullableCharacters'
        Long[] nullableLongs;
        if (in.getFieldKind("nullableCharacters") == FieldKind.ARRAY_OF_NULLABLE_INT64) {
            nullableLongs = in.readArrayOfNullableInt64("nullableCharacters");
        } else {
            nullableLongs = new Long[0];
        }
        Float[] nullableFloats;
        if (in.getFieldKind("nullableLongs") == FieldKind.ARRAY_OF_NULLABLE_FLOAT32) {
            nullableFloats = in.readArrayOfNullableFloat32("nullableLongs");
        } else {
            nullableFloats = new Float[0];
        }
        Double[] nullableDoubles;
        if (in.getFieldKind("nullableFloats") == FieldKind.ARRAY_OF_NULLABLE_FLOAT64) {
            nullableDoubles = in.readArrayOfNullableFloat64("nullableFloats");
        } else {
            nullableDoubles = new Double[0];
        }
        return new InnerDTO(bools, bytes, chars, shorts, ints, longs, floats, doubles, strings, namedDTOs,
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

    @Nonnull
    @Override
    public String getTypeName() {
        return "inner";
    }

    @Nonnull
    @Override
    public Class<InnerDTO> getCompactClass() {
        return InnerDTO.class;
    }
}
