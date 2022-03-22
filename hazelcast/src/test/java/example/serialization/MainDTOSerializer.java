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
import java.time.ZoneOffset;

public class MainDTOSerializer implements CompactSerializer<MainDTO> {
    @Nonnull
    @Override
    public MainDTO read(@Nonnull CompactReader reader) {
        boolean bool = reader.readBoolean("bool", false);
        byte b = reader.readInt8("b", (byte) 1);
        short s = reader.readInt16("s", (short) 1);
        int i = reader.readInt32("i", 1);
        long l = reader.readInt64("l", 1L);
        float f = reader.readFloat32("f", 1.0f);
        double d = reader.readFloat64("d", 1.0d);
        String str = reader.readString("str", "NA");
        InnerDTO p = reader.readCompact("p", null);
        BigDecimal bigDecimal = reader.readDecimal("bigDecimal", BigDecimal.ONE);
        LocalTime localTime = reader.readTime("localTime", LocalTime.of(1, 1, 1));
        LocalDate localDate = reader.readDate("localDate", LocalDate.of(1, 1, 1));
        LocalDateTime localDateTime = reader.readTimestamp("localDateTime",
                LocalDateTime.of(1, 1, 1, 1, 1, 1));
        OffsetDateTime offsetDateTime = reader.readTimestampWithTimezone("offsetDateTime",
                OffsetDateTime.of(1, 1, 1, 1, 1, 1, 1, ZoneOffset.ofHours(1)));
        Byte nullableB = reader.readNullableInt8("nullableB", (byte) 1);
        Boolean nullableBool = reader.readNullableBoolean("nullableBool", false);
        Short nullableS = reader.readNullableInt16("nullableS", (short) 1);
        Integer nullableI = reader.readNullableInt32("nullableI", 1);
        Long nullableL = reader.readNullableInt64("nullableL", 1L);
        Float nullableF = reader.readNullableFloat32("nullableF", 1.0f);
        Double nullableD = reader.readNullableFloat64("nullableD", 1.0d);
        return new MainDTO(b, bool, s, i, l, f, d, str, p, bigDecimal, localTime, localDate, localDateTime,
                offsetDateTime, nullableB, nullableBool, nullableS, nullableI, nullableL, nullableF, nullableD);
    }

    @Override
    public void write(@Nonnull CompactWriter out, @Nonnull MainDTO object) {
        out.writeBoolean("bool", object.bool);
        out.writeInt8("b", object.b);
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
        out.writeNullableInt16("nullableS", object.nullableS);
        out.writeNullableInt32("nullableI", object.nullableI);
        out.writeNullableInt64("nullableL", object.nullableL);
        out.writeNullableFloat32("nullableF", object.nullableF);
        out.writeNullableFloat64("nullableD", object.nullableD);
    }
}
