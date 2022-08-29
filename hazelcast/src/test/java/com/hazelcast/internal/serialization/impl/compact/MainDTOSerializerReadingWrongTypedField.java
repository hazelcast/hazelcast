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
import example.serialization.MainDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class MainDTOSerializerReadingWrongTypedField implements CompactSerializer<MainDTO> {
    @Nonnull
    @Override
    public MainDTO read(@Nonnull CompactReader reader) {
        // Every reader method tries to read the previous field.
        boolean bool;
        if (reader.getFieldKind("nullableD") == FieldKind.BOOLEAN) {
            bool = reader.readBoolean("nullableD");
        } else {
            bool = false;
        }
        byte b;
        if (reader.getFieldKind("bool") == FieldKind.INT8) {
            b = reader.readInt8("bool");
        } else {
            b = 1;
        }
        char c;
        if (reader.getFieldKind("b") == FieldKind.INT16) {
            c = (char) reader.readInt16("b");
        } else {
            c = 1;
        }
        // short and long reader methods are switched to make them read the wrong type
        // due to char and short using the same reader method
        short s;
        // this normally would be 'c' but switched with 'i'
        if (reader.getFieldKind("i") == FieldKind.INT16) {
            s = reader.readInt16("i");
        } else {
            s = 1;
        }
        int i;
        if (reader.getFieldKind("s") == FieldKind.INT32) {
            i = reader.readInt32("s");
        } else {
            i = 1;
        }
        long l;
        // this normally would be 'i' but switched with 'c'
        if (reader.getFieldKind("c") == FieldKind.INT64) {
            l = reader.readInt64("c");
        } else {
            l = 1L;
        }
        float f;
        if (reader.getFieldKind("l") == FieldKind.FLOAT32) {
            f = reader.readFloat32("l");
        } else {
            f = 1.0f;
        }
        double d;
        if (reader.getFieldKind("f") == FieldKind.FLOAT64) {
            d = reader.readFloat64("f");
        } else {
            d = 1.0d;
        }
        String str;
        if (reader.getFieldKind("d") == FieldKind.STRING) {
            str = reader.readString("d");
        } else {
            str = "NA";
        }
        InnerDTO p;
        if (reader.getFieldKind("str") == FieldKind.COMPACT) {
            p = reader.readCompact("str");
        } else {
            p = null;
        }
        BigDecimal bigDecimal;
        if (reader.getFieldKind("p") == FieldKind.DECIMAL) {
            bigDecimal = reader.readDecimal("p");
        } else {
            bigDecimal = BigDecimal.ONE;
        }
        LocalTime localTime;
        if (reader.getFieldKind("bigDecimal") == FieldKind.TIME) {
            localTime = reader.readTime("bigDecimal");
        } else {
            localTime = LocalTime.of(1, 1, 1);
        }
        LocalDate localDate;
        if (reader.getFieldKind("localTime") == FieldKind.DATE) {
            localDate = reader.readDate("localTime");
        } else {
            localDate = LocalDate.of(1, 1, 1);
        }
        LocalDateTime localDateTime;
        if (reader.getFieldKind("localDate") == FieldKind.TIMESTAMP) {
            localDateTime = reader.readTimestamp("localDate");
        } else {
            localDateTime = LocalDateTime.of(1, 1, 1, 1, 1, 1);
        }
        OffsetDateTime offsetDateTime;
        if (reader.getFieldKind("localDateTime") == FieldKind.TIMESTAMP_WITH_TIMEZONE) {
            offsetDateTime = reader.readTimestampWithTimezone("localDateTime");
        } else {
            offsetDateTime = OffsetDateTime.of(1, 1, 1, 1, 1, 1, 1, ZoneOffset.ofHours(1));
        }
        Byte nullableB;
        if (reader.getFieldKind("offsetDateTime") == FieldKind.NULLABLE_INT8) {
            nullableB = reader.readNullableInt8("offsetDateTime");
        } else {
            nullableB = 1;
        }
        Boolean nullableBool;
        if (reader.getFieldKind("nullableB") == FieldKind.NULLABLE_BOOLEAN) {
            nullableBool = reader.readNullableBoolean("nullableB");
        } else {
            nullableBool = false;
        }
        Character nullableC;
        if (reader.getFieldKind("nullableBool") == FieldKind.NULLABLE_INT16) {
            nullableC = CompactUtil.characterFromShort(reader.readNullableInt16("nullableBool"));
        } else {
            nullableC = 1;
        }
        // short and long reader methods are switched to make them read the wrong type
        // due to char and short using the same type
        Short nullableS;
        // this normally would be 'nullableC' but switched with 'nullableI'
        if (reader.getFieldKind("nullableI") == FieldKind.NULLABLE_INT16) {
            nullableS = reader.readNullableInt16("nullableI");
        } else {
            nullableS = 1;
        }
        Integer nullableI;
        if (reader.getFieldKind("nullableS") == FieldKind.NULLABLE_INT32) {
            nullableI = reader.readNullableInt32("nullableS");
        } else {
            nullableI = 1;
        }
        Long nullableL;
        // this normally would be 'nullableI' but switched with 'nullableC'
        if (reader.getFieldKind("nullableC") == FieldKind.NULLABLE_INT64) {
            nullableL = reader.readNullableInt64("nullableC");
        } else {
            nullableL = 1L;
        }
        Float nullableF;
        if (reader.getFieldKind("nullableL") == FieldKind.NULLABLE_FLOAT32) {
            nullableF = reader.readNullableFloat32("nullableL");
        } else {
            nullableF = 1.0f;
        }
        Double nullableD;
        if (reader.getFieldKind("nullableF") == FieldKind.NULLABLE_FLOAT64) {
            nullableD = reader.readNullableFloat64("nullableF");
        } else {
            nullableD = 1.0d;
        }
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
