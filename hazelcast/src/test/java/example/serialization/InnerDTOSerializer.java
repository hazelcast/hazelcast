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
package example.serialization;

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
    public InnerDTO read(@Nonnull CompactReader in) {
        boolean[] bools = in.readArrayOfBoolean("bools", new boolean[0]);
        byte[] bytes = in.readArrayOfInt8("bytes", new byte[0]);
        short[] shorts = in.readArrayOfInt16("shorts", new short[0]);
        int[] ints = in.readArrayOfInt32("ints", new int[0]);
        long[] longs = in.readArrayOfInt64("longs", new long[0]);
        float[] floats = in.readArrayOfFloat32("floats", new float[0]);
        double[] doubles = in.readArrayOfFloat64("doubles", new double[0]);
        String[] strings = in.readArrayOfString("strings", new String[0]);
        NamedDTO[] namedDTOS = in.readArrayOfCompact("nn", NamedDTO.class, new NamedDTO[0]);
        BigDecimal[] bigDecimals = in.readArrayOfDecimal("bigDecimals", new BigDecimal[0]);
        LocalTime[] localTimes = in.readArrayOfTime("localTimes", new LocalTime[0]);
        LocalDate[] localDates = in.readArrayOfDate("localDates", new LocalDate[0]);
        LocalDateTime[] localDateTimes = in.readArrayOfTimestamp("localDateTimes", new LocalDateTime[0]);
        OffsetDateTime[] offsetDateTimes = in.readArrayOfTimestampWithTimezone("offsetDateTimes", new OffsetDateTime[0]);
        Boolean[] nullableBools = in.readArrayOfNullableBoolean("nullableBools", new Boolean[0]);
        Byte[] nullableBytes = in.readArrayOfNullableInt8("nullableBytes", new Byte[0]);
        Short[] nullableShorts = in.readArrayOfNullableInt16("nullableShorts", new Short[0]);
        Integer[] nullableIntegers = in.readArrayOfNullableInt32("nullableIntegers", new Integer[0]);
        Long[] nullableLongs = in.readArrayOfNullableInt64("nullableLongs", new Long[0]);
        Float[] nullableFloats = in.readArrayOfNullableFloat32("nullableFloats", new Float[0]);
        Double[] nullableDoubles = in.readArrayOfNullableFloat64("nullableDoubles", new Double[0]);
        return new InnerDTO(bools, bytes, shorts, ints, longs, floats, doubles, strings, namedDTOS,
                bigDecimals, localTimes, localDates, localDateTimes, offsetDateTimes, nullableBools, nullableBytes,
                nullableShorts, nullableIntegers, nullableLongs, nullableFloats, nullableDoubles);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull InnerDTO object) {
        out.writeArrayOfBoolean("bools", object.bools);
        out.writeArrayOfInt8("bytes", object.bytes);
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
        out.writeArrayOfNullableInt16("nullableShorts", object.nullableShorts);
        out.writeArrayOfNullableInt32("nullableIntegers", object.nullableIntegers);
        out.writeArrayOfNullableInt64("nullableLongs", object.nullableLongs);
        out.writeArrayOfNullableFloat32("nullableFloats", object.nullableFloats);
        out.writeArrayOfNullableFloat64("nullableDoubles", object.nullableDoubles);
    }
}
