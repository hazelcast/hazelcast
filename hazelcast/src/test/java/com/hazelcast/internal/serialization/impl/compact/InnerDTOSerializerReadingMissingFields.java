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

public class InnerDTOSerializerReadingMissingFields implements CompactSerializer<InnerDTO> {
    @Nonnull
    @Override
    public InnerDTO read(@Nonnull CompactReader in) {
        boolean[] bools;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_BOOLEAN) {
            bools = in.readArrayOfBoolean("nonExistingField");
        } else {
            bools = new boolean[0];
        }
        byte[] bytes;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_INT8) {
            bytes = in.readArrayOfInt8("nonExistingField");
        } else {
            bytes = new byte[0];
        }
        short[] shorts;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_INT16) {
            shorts = in.readArrayOfInt16("nonExistingField");
        } else {
            shorts = new short[0];
        }
        char[] chars;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_INT16) {
            chars = CompactUtil.charArrayFromShortArray(in.readArrayOfInt16("nonExistingField"));
        } else {
            chars = CompactUtil.charArrayFromShortArray(new short[0]);
        }
        int[] ints;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_INT32) {
            ints = in.readArrayOfInt32("nonExistingField");
        } else {
            ints = new int[0];
        }
        long[] longs;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_INT64) {
            longs = in.readArrayOfInt64("nonExistingField");
        } else {
            longs = new long[0];
        }
        float[] floats;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_FLOAT32) {
            floats = in.readArrayOfFloat32("nonExistingField");
        } else {
            floats = new float[0];
        }
        double[] doubles;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_FLOAT64) {
            doubles = in.readArrayOfFloat64("nonExistingField");
        } else {
            doubles = new double[0];
        }
        String[] strings;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_STRING) {
            strings = in.readArrayOfString("nonExistingField");
        } else {
            strings = new String[0];
        }
        NamedDTO[] namedDTOs;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_COMPACT) {
            namedDTOs = in.readArrayOfCompact("nonExistingField", NamedDTO.class);
        } else {
            namedDTOs = new NamedDTO[0];
        }
        BigDecimal[] bigDecimals;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_DECIMAL) {
            bigDecimals = in.readArrayOfDecimal("nonExistingField");
        } else {
            bigDecimals = new BigDecimal[0];
        }
        LocalTime[] localTimes;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_TIME) {
            localTimes = in.readArrayOfTime("nonExistingField");
        } else {
            localTimes = new LocalTime[0];
        }
        LocalDate[] localDates;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_DATE) {
            localDates = in.readArrayOfDate("nonExistingField");
        } else {
            localDates = new LocalDate[0];
        }
        LocalDateTime[] localDateTimes;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_TIMESTAMP) {
            localDateTimes = in.readArrayOfTimestamp("nonExistingField");
        } else {
            localDateTimes = new LocalDateTime[0];
        }
        OffsetDateTime[] offsetDateTimes;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE) {
            offsetDateTimes = in.readArrayOfTimestampWithTimezone("nonExistingField");
        } else {
            offsetDateTimes = new OffsetDateTime[0];
        }
        Boolean[] nullableBools;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_BOOLEAN) {
            nullableBools = in.readArrayOfNullableBoolean("nonExistingField");
        } else {
            nullableBools = new Boolean[0];
        }
        Byte[] nullableBytes;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_INT8) {
            nullableBytes = in.readArrayOfNullableInt8("nonExistingField");
        } else {
            nullableBytes = new Byte[0];
        }
        Short[] nullableCharactersAsShorts;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_INT16) {
            nullableCharactersAsShorts = in.readArrayOfNullableInt16("nonExistingField");
        } else {
            nullableCharactersAsShorts = new Short[0];
        }
        Character[] nullableCharacters = CompactUtil.characterArrayFromShortArray(nullableCharactersAsShorts);
        Short[] nullableShorts;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_INT16) {
            nullableShorts = in.readArrayOfNullableInt16("nonExistingField");
        } else {
            nullableShorts = new Short[0];
        }
        Integer[] nullableIntegers;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_INT32) {
            nullableIntegers = in.readArrayOfNullableInt32("nonExistingField");
        } else {
            nullableIntegers = new Integer[0];
        }
        Long[] nullableLongs;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_INT64) {
            nullableLongs = in.readArrayOfNullableInt64("nonExistingField");
        } else {
            nullableLongs = new Long[0];
        }
        Float[] nullableFloats;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_FLOAT32) {
            nullableFloats = in.readArrayOfNullableFloat32("nonExistingField");
        } else {
            nullableFloats = new Float[0];
        }
        Double[] nullableDoubles;
        if (in.getFieldKind("nonExistingField") == FieldKind.ARRAY_OF_NULLABLE_FLOAT64) {
            nullableDoubles = in.readArrayOfNullableFloat64("nonExistingField");
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
