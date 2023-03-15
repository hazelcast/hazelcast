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
import example.serialization.InnerDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class VarSizedFieldsDTOSerializer implements CompactSerializer<VarSizedFieldsDTO> {
    @Nonnull
    @Override
    public VarSizedFieldsDTO read(@Nonnull CompactReader reader) {
        boolean[] arrayOfBoolean = reader.readArrayOfBoolean("arrayOfBoolean");
        byte[] arrayOfInt8 = reader.readArrayOfInt8("arrayOfInt8");
        short[] arrayOfInt16 = reader.readArrayOfInt16("arrayOfInt16");
        int[] arrayOfInt32 = reader.readArrayOfInt32("arrayOfInt32");
        long[] arrayOfInt64 = reader.readArrayOfInt64("arrayOfInt64");
        float[] arrayOfFloat32 = reader.readArrayOfFloat32("arrayOfFloat32");
        double[] arrayOfFloat64 = reader.readArrayOfFloat64("arrayOfFloat64");
        String str = reader.readString("str");
        String[] arrayOfString = reader.readArrayOfString("arrayOfString");
        BigDecimal bigDecimal = reader.readDecimal("bigDecimal");
        BigDecimal[] arrayOfBigDecimal = reader.readArrayOfDecimal("arrayOfBigDecimal");
        LocalTime localTime = reader.readTime("localTime");
        LocalTime[] arrayOfLocalTime = reader.readArrayOfTime("arrayOfLocalTime");
        LocalDate localDate = reader.readDate("localDate");
        LocalDate[] arrayOfLocalDate = reader.readArrayOfDate("arrayOfLocalDate");
        LocalDateTime localDateTime = reader.readTimestamp("localDateTime");
        LocalDateTime[] arrayOfLocalDateTime = reader.readArrayOfTimestamp("arrayOfLocalDateTime");
        OffsetDateTime offsetDateTime = reader.readTimestampWithTimezone("offsetDateTime");
        OffsetDateTime[] arrayOfOffsetDateTime = reader.readArrayOfTimestampWithTimezone("arrayOfOffsetDateTime");
        InnerDTO compact = reader.readCompact("compact");
        InnerDTO[] arrayOfCompact = reader.readArrayOfCompact("arrayOfCompact", InnerDTO.class);
        Boolean nullableBool = reader.readNullableBoolean("nullableBool");
        Boolean[] arrayOfNullableBool = reader.readArrayOfNullableBoolean("arrayOfNullableBool");
        Byte nullableB = reader.readNullableInt8("nullableB");
        Byte[] arrayOfNullableB = reader.readArrayOfNullableInt8("arrayOfNullableB");
        Short nullableS = reader.readNullableInt16("nullableS");
        Short[] arrayOfNullableS = reader.readArrayOfNullableInt16("arrayOfNullableS");
        Integer nullableI = reader.readNullableInt32("nullableI");
        Integer[] arrayOfNullableI = reader.readArrayOfNullableInt32("arrayOfNullableI");
        Long nullableL = reader.readNullableInt64("nullableL");
        Long[] arrayOfNullableL = reader.readArrayOfNullableInt64("arrayOfNullableL");
        Float nullableF = reader.readNullableFloat32("nullableF");
        Float[] arrayOfNullableF = reader.readArrayOfNullableFloat32("arrayOfNullableF");
        Double nullableD = reader.readNullableFloat64("nullableD");
        Double[] arrayOfNullableD = reader.readArrayOfNullableFloat64("arrayOfNullableD");

        return new VarSizedFieldsDTO(arrayOfBoolean, arrayOfInt8, arrayOfInt16, arrayOfInt32, arrayOfInt64, arrayOfFloat32,
                arrayOfFloat64, str, arrayOfString, bigDecimal, arrayOfBigDecimal, localTime, arrayOfLocalTime,
                localDate, arrayOfLocalDate, localDateTime, arrayOfLocalDateTime, offsetDateTime,
                arrayOfOffsetDateTime, compact, arrayOfCompact, nullableBool, arrayOfNullableBool, nullableB,
                arrayOfNullableB, nullableS, arrayOfNullableS, nullableI, arrayOfNullableI, nullableL,
                arrayOfNullableL, nullableF, arrayOfNullableF, nullableD, arrayOfNullableD
        );
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull VarSizedFieldsDTO object) {
        out.writeArrayOfBoolean("arrayOfBoolean", object.arrayOfBoolean);
        out.writeArrayOfInt8("arrayOfInt8", object.arrayOfInt8);
        out.writeArrayOfInt16("arrayOfInt16", object.arrayOfInt16);
        out.writeArrayOfInt32("arrayOfInt32", object.arrayOfInt32);
        out.writeArrayOfInt64("arrayOfInt64", object.arrayOfInt64);
        out.writeArrayOfFloat32("arrayOfFloat32", object.arrayOfFloat32);
        out.writeArrayOfFloat64("arrayOfFloat64", object.arrayOfFloat64);
        out.writeString("str", object.str);
        out.writeArrayOfString("arrayOfString", object.arrayOfString);
        out.writeDecimal("bigDecimal", object.bigDecimal);
        out.writeArrayOfDecimal("arrayOfBigDecimal", object.arrayOfBigDecimal);
        out.writeTime("localTime", object.localTime);
        out.writeArrayOfTime("arrayOfLocalTime", object.arrayOfLocalTime);
        out.writeDate("localDate", object.localDate);
        out.writeArrayOfDate("arrayOfLocalDate", object.arrayOfLocalDate);
        out.writeTimestamp("localDateTime", object.localDateTime);
        out.writeArrayOfTimestamp("arrayOfLocalDateTime", object.arrayOfLocalDateTime);
        out.writeTimestampWithTimezone("offsetDateTime", object.offsetDateTime);
        out.writeArrayOfTimestampWithTimezone("arrayOfOffsetDateTime", object.arrayOfOffsetDateTime);
        out.writeCompact("compact", object.compact);
        out.writeArrayOfCompact("arrayOfCompact", object.arrayOfCompact);
        out.writeNullableBoolean("nullableBool", object.nullableBool);
        out.writeArrayOfNullableBoolean("arrayOfNullableBool", object.arrayOfNullableBool);
        out.writeNullableInt8("nullableB", object.nullableB);
        out.writeArrayOfNullableInt8("arrayOfNullableB", object.arrayOfNullableB);
        out.writeNullableInt16("nullableS", object.nullableS);
        out.writeArrayOfNullableInt16("arrayOfNullableS", object.arrayOfNullableS);
        out.writeNullableInt32("nullableI", object.nullableI);
        out.writeArrayOfNullableInt32("arrayOfNullableI", object.arrayOfNullableI);
        out.writeNullableInt64("nullableL", object.nullableL);
        out.writeArrayOfNullableInt64("arrayOfNullableL", object.arrayOfNullableL);
        out.writeNullableFloat32("nullableF", object.nullableF);
        out.writeArrayOfNullableFloat32("arrayOfNullableF", object.arrayOfNullableF);
        out.writeNullableFloat64("nullableD", object.nullableD);
        out.writeArrayOfNullableFloat64("arrayOfNullableD", object.arrayOfNullableD);
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "varSizedFields";
    }

    @Nonnull
    @Override
    public Class<VarSizedFieldsDTO> getCompactClass() {
        return VarSizedFieldsDTO.class;
    }
}
