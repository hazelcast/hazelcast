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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;

public class MainPortable implements Portable {

    public byte b;
    public boolean bool;
    public char c;
    public short s;
    public int i;
    public long l;
    public float f;
    public double d;
    public String str;
    public InnerPortable p;
    public BigDecimal bigDecimal;
    public LocalTime localTime;
    public LocalDate localDate;
    public LocalDateTime localDateTime;
    public OffsetDateTime offsetDateTime;

    MainPortable() {
    }

    public MainPortable(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str, InnerPortable p,
                        BigDecimal bigDecimal, LocalTime localTime, LocalDate localDate,
                        LocalDateTime localDateTime, OffsetDateTime offsetDateTime) {
        this.b = b;
        this.bool = bool;
        this.c = c;
        this.s = s;
        this.i = i;
        this.l = l;
        this.f = f;
        this.d = d;
        this.str = str;
        this.p = p;
        this.bigDecimal = bigDecimal;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.offsetDateTime = offsetDateTime;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.MAIN_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeByte("b", b);
        writer.writeBoolean("bool", bool);
        writer.writeChar("c", c);
        writer.writeShort("s", s);
        writer.writeInt("i", i);
        writer.writeLong("l", l);
        writer.writeFloat("f", f);
        writer.writeDouble("d", d);
        writer.writeString("str", str);
        if (p != null) {
            writer.writePortable("p", p);
        } else {
            writer.writeNullPortable("p", TestSerializationConstants.PORTABLE_FACTORY_ID,
                    TestSerializationConstants.INNER_PORTABLE);
        }
        writer.writeDecimal("bigDecimal", bigDecimal);
        writer.writeTime("localTime", localTime);
        writer.writeDate("localDate", localDate);
        writer.writeTimestamp("localDateTime", localDateTime);
        writer.writeTimestampWithTimezone("offsetDateTime", offsetDateTime);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        b = reader.readByte("b");
        bool = reader.readBoolean("bool");
        c = reader.readChar("c");
        s = reader.readShort("s");
        i = reader.readInt("i");
        l = reader.readLong("l");
        f = reader.readFloat("f");
        d = reader.readDouble("d");
        str = reader.readString("str");
        p = reader.readPortable("p");
        bigDecimal = reader.readDecimal("bigDecimal");
        localTime = reader.readTime("localTime");
        localDate = reader.readDate("localDate");
        localDateTime = reader.readTimestamp("localDateTime");
        offsetDateTime = reader.readTimestampWithTimezone("offsetDateTime");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MainPortable that = (MainPortable) o;
        return b == that.b
                && bool == that.bool
                && c == that.c
                && s == that.s
                && i == that.i
                && l == that.l
                && Float.compare(that.f, f) == 0
                && Double.compare(that.d, d) == 0
                && Objects.equals(str, that.str)
                && Objects.equals(p, that.p)
                && Objects.equals(bigDecimal, that.bigDecimal)
                && Objects.equals(localTime, that.localTime)
                && Objects.equals(localDate, that.localDate)
                && Objects.equals(localDateTime, that.localDateTime)
                && Objects.equals(offsetDateTime, that.offsetDateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(b, bool, c, s, i, l, f, d, str, p, bigDecimal, localTime, localDate,
                localDateTime, offsetDateTime);
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }
}
