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
import java.time.ZoneOffset;

public class MainDTOSerializer implements CompactSerializer<MainDTO> {
    @Nonnull
    @Override
    public MainDTO read(@Nonnull CompactReader reader) {
        boolean bool = reader.getFieldKind("bool") == FieldKind.BOOLEAN
                ? reader.readBoolean("bool")
                : false;

        byte b = reader.getFieldKind("b") == FieldKind.INT8
                ? reader.readInt8("b")
                : 1;

        char c = reader.getFieldKind("c") == FieldKind.INT16
                ? (char) reader.readInt16("c")
                : 1;

        short s = reader.getFieldKind("s") == FieldKind.INT16
                ? reader.readInt16("s")
                : 1;

        int i = reader.getFieldKind("i") == FieldKind.INT32
                ? reader.readInt32("i")
                : 1;

        long l = reader.getFieldKind("l") == FieldKind.INT64
                ? reader.readInt64("l")
                : 1;

        float f = reader.getFieldKind("f") == FieldKind.FLOAT32
                ? reader.readFloat32("f")
                : 1.0F;

        double d = reader.getFieldKind("d") == FieldKind.FLOAT64
                ? reader.readFloat64("d")
                : 1.0;

        String str = reader.getFieldKind("str") == FieldKind.STRING
                ? reader.readString("str")
                : "NA";

        InnerDTO p = reader.getFieldKind("p") == FieldKind.COMPACT
                ? reader.readCompact("p")
                : null;

        BigDecimal bigDecimal = reader.getFieldKind("bigDecimal") == FieldKind.DECIMAL
                ? reader.readDecimal("bigDecimal")
                : BigDecimal.ONE;

        LocalTime localTime = reader.getFieldKind("localTime") == FieldKind.TIME
                ? reader.readTime("localTime")
                : LocalTime.of(1, 1, 1);

        LocalDate localDate = reader.getFieldKind("localDate") == FieldKind.DATE
                ? reader.readDate("localDate")
                : LocalDate.of(1, 1, 1);

        LocalDateTime localDateTime = reader.getFieldKind("localDateTime") == FieldKind.TIMESTAMP
                ? reader.readTimestamp("localDateTime")
                : LocalDateTime.of(1, 1, 1, 1, 1, 1);

        OffsetDateTime offsetDateTime = reader.getFieldKind("offsetDateTime") == FieldKind.TIMESTAMP_WITH_TIMEZONE
                ? reader.readTimestampWithTimezone("offsetDateTime")
                : OffsetDateTime.of(1, 1, 1, 1, 1, 1, 1, ZoneOffset.ofHours(1));

        Byte nullableB = reader.getFieldKind("nullableB") == FieldKind.NULLABLE_INT8
                ? reader.readNullableInt8("nullableB")
                : Byte.valueOf((byte) 1);

        Boolean nullableBool = reader.getFieldKind("nullableBool") == FieldKind.NULLABLE_BOOLEAN
                ? reader.readNullableBoolean("nullableBool")
                : Boolean.FALSE;

        Character nullableC = reader.getFieldKind("nullableC") == FieldKind.NULLABLE_INT16
                ? CompactUtil.characterFromShort(reader.readNullableInt16("nullableC"))
                : Character.valueOf((char) 1);

        Short nullableS = reader.getFieldKind("nullableS") == FieldKind.NULLABLE_INT16
                ? reader.readNullableInt16("nullableS")
                : Short.valueOf((short) 1);

        Integer nullableI = reader.getFieldKind("nullableI") == FieldKind.NULLABLE_INT32
                ? reader.readNullableInt32("nullableI")
                : Integer.valueOf(1);

        Long nullableL = reader.getFieldKind("nullableL") == FieldKind.NULLABLE_INT64
                ? reader.readNullableInt64("nullableL")
                : Long.valueOf(1);

        Float nullableF = reader.getFieldKind("nullableF") == FieldKind.NULLABLE_FLOAT32
                ? reader.readNullableFloat32("nullableF")
                : Float.valueOf(1.0F);

        Double nullableD = reader.getFieldKind("nullableD") == FieldKind.NULLABLE_FLOAT64
                ? reader.readNullableFloat64("nullableD")
                : Double.valueOf(1.0);

        return new MainDTO(b, bool, c, s, i, l, f, d, str, p, bigDecimal, localTime, localDate, localDateTime,
                offsetDateTime, nullableB, nullableBool, nullableC, nullableS, nullableI, nullableL, nullableF, nullableD);
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull MainDTO object) {
        writer.writeBoolean("bool", object.bool);
        writer.writeInt8("b", object.b);
        writer.writeInt16("c", (short) object.c);
        writer.writeInt16("s", object.s);
        writer.writeInt32("i", object.i);
        writer.writeInt64("l", object.l);
        writer.writeFloat32("f", object.f);
        writer.writeFloat64("d", object.d);
        writer.writeString("str", object.str);
        writer.writeCompact("p", object.p);
        writer.writeDecimal("bigDecimal", object.bigDecimal);
        writer.writeTime("localTime", object.localTime);
        writer.writeDate("localDate", object.localDate);
        writer.writeTimestamp("localDateTime", object.localDateTime);
        writer.writeTimestampWithTimezone("offsetDateTime", object.offsetDateTime);
        writer.writeNullableInt8("nullableB", object.nullableB);
        writer.writeNullableBoolean("nullableBool", object.nullableBool);
        writer.writeNullableInt16("nullableC", CompactUtil.characterAsShort(object.nullableC));
        writer.writeNullableInt16("nullableS", object.nullableS);
        writer.writeNullableInt32("nullableI", object.nullableI);
        writer.writeNullableInt64("nullableL", object.nullableL);
        writer.writeNullableFloat32("nullableF", object.nullableF);
        writer.writeNullableFloat64("nullableD", object.nullableD);
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "main";
    }

    @Nonnull
    @Override
    public Class<MainDTO> getCompactClass() {
        return MainDTO.class;
    }
}
