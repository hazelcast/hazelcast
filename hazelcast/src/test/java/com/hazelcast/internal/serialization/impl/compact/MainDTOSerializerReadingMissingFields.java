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
import example.serialization.MainDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class MainDTOSerializerReadingMissingFields implements CompactSerializer<MainDTO> {
    @Nonnull
    @Override
    public MainDTO read(@Nonnull CompactReader reader) {
        boolean bool = reader.readBoolean("nonExistingField", false);
        byte b = reader.readInt8("nonExistingField", (byte) 1);
        char c = (char) reader.readInt16("nonExistingField", (short) 1);
        short s = reader.readInt16("nonExistingField", (short) 1);
        int i = reader.readInt32("nonExistingField", 1);
        long l = reader.readInt64("nonExistingField", 1L);
        float f = reader.readFloat32("nonExistingField", 1.0f);
        double d = reader.readFloat64("nonExistingField", 1.0d);
        String str = reader.readString("nonExistingField", "NA");
        InnerDTO p = reader.readCompact("nonExistingField", null);
        BigDecimal bigDecimal = reader.readDecimal("nonExistingField", BigDecimal.ONE);
        LocalTime localTime = reader.readTime("nonExistingField", LocalTime.of(1, 1, 1));
        LocalDate localDate = reader.readDate("nonExistingField", LocalDate.of(1, 1, 1));
        LocalDateTime localDateTime = reader.readTimestamp("nonExistingField",
                LocalDateTime.of(1, 1, 1, 1, 1, 1));
        OffsetDateTime offsetDateTime = reader.readTimestampWithTimezone("nonExistingField",
                OffsetDateTime.of(1, 1, 1, 1, 1, 1, 1, ZoneOffset.ofHours(1)));
        Byte nullableB = reader.readNullableInt8("nonExistingField", (byte) 1);
        Boolean nullableBool = reader.readNullableBoolean("nonExistingField", false);
        Character nullableC = CompactUtil.characterFromShort(reader.readNullableInt16("nonExistingField", (short) 1));
        Short nullableS = reader.readNullableInt16("nonExistingField", (short) 1);
        Integer nullableI = reader.readNullableInt32("nonExistingField", 1);
        Long nullableL = reader.readNullableInt64("nonExistingField", 1L);
        Float nullableF = reader.readNullableFloat32("nonExistingField", 1.0f);
        Double nullableD = reader.readNullableFloat64("nonExistingField", 1.0d);
        return new MainDTO(b, bool, c, s, i, l, f, d, str, p, bigDecimal, localTime, localDate, localDateTime,
                offsetDateTime, nullableB, nullableBool, nullableC, nullableS, nullableI, nullableL, nullableF, nullableD);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull MainDTO object) {
        out.writeBoolean("bool", object.bool);
        out.writeInt8("b", object.b);
        out.writeInt16("c", (short) object.c);
        out.writeInt16("s", object.s);
        out.writeInt32("i", object.i);
        out.writeInt64("l", object.l);
        out.writeFloat32("f", object.f);
        out.writeFloat64("d", object.d);
        out.writeString("str", object.str);
        out.writeCompact("p", object.p);
        out.writeDecimal("bigDecimal", object.bigDecimal);
        out.writeTime("localTime", object.localTime);
        out.writeDate("localDate", object.localDate);
        out.writeTimestamp("localDateTime", object.localDateTime);
        out.writeTimestampWithTimezone("offsetDateTime", object.offsetDateTime);
        out.writeNullableInt8("nullableB", object.nullableB);
        out.writeNullableBoolean("nullableBool", object.nullableBool);
        out.writeNullableInt16("nullableC", CompactUtil.characterAsShort(object.nullableC));
        out.writeNullableInt16("nullableS", object.nullableS);
        out.writeNullableInt32("nullableI", object.nullableI);
        out.writeNullableInt64("nullableL", object.nullableL);
        out.writeNullableFloat32("nullableF", object.nullableF);
        out.writeNullableFloat64("nullableD", object.nullableD);
    }
}
