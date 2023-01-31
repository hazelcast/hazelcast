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
package example.serialization;

import com.hazelcast.internal.serialization.impl.compact.CompactUtil;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class InnerDTOSerializer implements CompactSerializer<InnerDTO> {
    @Nonnull
    @Override
    public InnerDTO read(@Nonnull CompactReader reader) {
        boolean[] bools = reader.getFieldKind("bools") == FieldKind.ARRAY_OF_BOOLEAN
                ? reader.readArrayOfBoolean("bools")
                : new boolean[0];

        byte[] bytes = reader.getFieldKind("bytes") == FieldKind.ARRAY_OF_INT8
                ? reader.readArrayOfInt8("bytes")
                : new byte[0];

        short[] shorts = reader.getFieldKind("shorts") == FieldKind.ARRAY_OF_INT16
                ? reader.readArrayOfInt16("shorts")
                : new short[0];

        char[] chars = reader.getFieldKind("chars") == FieldKind.ARRAY_OF_INT16
                ? CompactUtil.charArrayFromShortArray(reader.readArrayOfInt16("chars"))
                : new char[0];

        int[] ints = reader.getFieldKind("ints") == FieldKind.ARRAY_OF_INT32
                ? reader.readArrayOfInt32("ints")
                : new int[0];

        long[] longs = reader.getFieldKind("longs") == FieldKind.ARRAY_OF_INT64
                ? reader.readArrayOfInt64("longs")
                : new long[0];

        float[] floats = reader.getFieldKind("floats") == FieldKind.ARRAY_OF_FLOAT32
                ? reader.readArrayOfFloat32("floats")
                : new float[0];

        double[] doubles = reader.getFieldKind("doubles") == FieldKind.ARRAY_OF_FLOAT64
                ? reader.readArrayOfFloat64("doubles")
                : new double[0];

        String[] strings = reader.getFieldKind("strings") == FieldKind.ARRAY_OF_STRING
                ? reader.readArrayOfString("strings")
                : new String[0];

        NamedDTO[] namedDTOS = reader.getFieldKind("nn") == FieldKind.ARRAY_OF_COMPACT
                ? reader.readArrayOfCompact("nn", NamedDTO.class)
                : new NamedDTO[0];

        BigDecimal[] bigDecimals = reader.getFieldKind("bigDecimals") == FieldKind.ARRAY_OF_DECIMAL
                ? reader.readArrayOfDecimal("bigDecimals")
                : new BigDecimal[0];

        LocalTime[] localTimes = reader.getFieldKind("localTimes") == FieldKind.ARRAY_OF_TIME
                ? reader.readArrayOfTime("localTimes")
                : new LocalTime[0];

        LocalDate[] localDates = reader.getFieldKind("localDates") == FieldKind.ARRAY_OF_DATE
                ? reader.readArrayOfDate("localDates")
                : new LocalDate[0];

        LocalDateTime[] localDateTimes = reader.getFieldKind("localDateTimes") == FieldKind.ARRAY_OF_TIMESTAMP
                ? reader.readArrayOfTimestamp("localDateTimes")
                : new LocalDateTime[0];

        OffsetDateTime[] offsetDateTimes = reader.getFieldKind("offsetDateTimes") == FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE
                ? reader.readArrayOfTimestampWithTimezone("offsetDateTimes")
                : new OffsetDateTime[0];

        Boolean[] nullableBools = reader.getFieldKind("nullableBools") == FieldKind.ARRAY_OF_NULLABLE_BOOLEAN
                ? reader.readArrayOfNullableBoolean("nullableBools")
                : new Boolean[0];

        Byte[] nullableBytes = reader.getFieldKind("nullableBytes") == FieldKind.ARRAY_OF_NULLABLE_INT8
                ? reader.readArrayOfNullableInt8("nullableBytes")
                : new Byte[0];

        Character[] nullableCharacters = reader.getFieldKind("nullableCharacters") == FieldKind.ARRAY_OF_NULLABLE_INT16
                ? CompactUtil.characterArrayFromShortArray(reader.readArrayOfNullableInt16("nullableCharacters"))
                : new Character[0];

        Short[] nullableShorts = reader.getFieldKind("nullableShorts") == FieldKind.ARRAY_OF_NULLABLE_INT16
                ? reader.readArrayOfNullableInt16("nullableShorts")
                : new Short[0];

        Integer[] nullableIntegers = reader.getFieldKind("nullableIntegers") == FieldKind.ARRAY_OF_NULLABLE_INT32
                ? reader.readArrayOfNullableInt32("nullableIntegers")
                : new Integer[0];

        Long[] nullableLongs = reader.getFieldKind("nullableLongs") == FieldKind.ARRAY_OF_NULLABLE_INT64
                ? reader.readArrayOfNullableInt64("nullableLongs")
                : new Long[0];

        Float[] nullableFloats = reader.getFieldKind("nullableFloats") == FieldKind.ARRAY_OF_NULLABLE_FLOAT32
                ? reader.readArrayOfNullableFloat32("nullableFloats")
                : new Float[0];

        Double[] nullableDoubles = reader.getFieldKind("nullableDoubles") == FieldKind.ARRAY_OF_NULLABLE_FLOAT64
                ? reader.readArrayOfNullableFloat64("nullableDoubles")
                : new Double[0];

        return new InnerDTO(bools, bytes, chars, shorts, ints, longs, floats, doubles, strings, namedDTOS,
                bigDecimals, localTimes, localDates, localDateTimes, offsetDateTimes, nullableBools, nullableBytes,
                nullableCharacters, nullableShorts, nullableIntegers, nullableLongs, nullableFloats, nullableDoubles);
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull InnerDTO object) {
        writer.writeArrayOfBoolean("bools", object.bools);
        writer.writeArrayOfInt8("bytes", object.bytes);
        writer.writeArrayOfInt16("chars", CompactUtil.charArrayAsShortArray(object.chars));
        writer.writeArrayOfInt16("shorts", object.shorts);
        writer.writeArrayOfInt32("ints", object.ints);
        writer.writeArrayOfInt64("longs", object.longs);
        writer.writeArrayOfFloat32("floats", object.floats);
        writer.writeArrayOfFloat64("doubles", object.doubles);
        writer.writeArrayOfString("strings", object.strings);
        writer.writeArrayOfCompact("nn", object.nn);
        writer.writeArrayOfDecimal("bigDecimals", object.bigDecimals);
        writer.writeArrayOfTime("localTimes", object.localTimes);
        writer.writeArrayOfDate("localDates", object.localDates);
        writer.writeArrayOfTimestamp("localDateTimes", object.localDateTimes);
        writer.writeArrayOfTimestampWithTimezone("offsetDateTimes", object.offsetDateTimes);
        writer.writeArrayOfNullableBoolean("nullableBools", object.nullableBools);
        writer.writeArrayOfNullableInt8("nullableBytes", object.nullableBytes);
        writer.writeArrayOfNullableInt16("nullableCharacters", CompactUtil.characterArrayAsShortArray(object.nullableCharacters));
        writer.writeArrayOfNullableInt16("nullableShorts", object.nullableShorts);
        writer.writeArrayOfNullableInt32("nullableIntegers", object.nullableIntegers);
        writer.writeArrayOfNullableInt64("nullableLongs", object.nullableLongs);
        writer.writeArrayOfNullableFloat32("nullableFloats", object.nullableFloats);
        writer.writeArrayOfNullableFloat64("nullableDoubles", object.nullableDoubles);
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
