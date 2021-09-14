/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.google.common.base.Objects;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public class MainDTO {

    public byte b;
    public int ub;
    public boolean bool;
    public char c;
    public short s;
    public int us;
    public int i;
    public long ui;
    public long l;
    public BigInteger ul;
    public float f;
    public double d;
    public String str;
    public InnerDTO p;
    public BigDecimal bigDecimal;
    public LocalTime localTime;
    public LocalDate localDate;
    public LocalDateTime localDateTime;
    public OffsetDateTime offsetDateTime;

    public static class MainDTOSerializer implements CompactSerializer<MainDTO> {
        @NotNull
        @Override
        public MainDTO read(@NotNull CompactReader in) throws IOException {

            return new MainDTO(in.readByte("b"), in.readUnsignedByte("ub"), in.readBoolean("bool"),
                    in.readChar("c"), in.readShort("s"), in.readUnsignedShort("us"), in.readInt("i"),
                    in.readUnsignedInt("ui"), in.readLong("l"), in.readUnsignedLong("ul"), in.readFloat("f"),
                    in.readDouble("d"), in.readString("str"), in.readObject("p"), in.readDecimal("bigDecimal"),
                    in.readTime("localTime"), in.readDate("localDate"), in.readTimestamp("localDateTime"),
                    in.readTimestampWithTimezone("offsetDateTime"));
        }

        @Override
        public void write(@NotNull CompactWriter out, @NotNull MainDTO o) throws IOException {
            out.writeByte("b", o.b);
            out.writeUnsignedByte("ub", o.ub);
            out.writeBoolean("bool", o.bool);
            out.writeChar("c", o.c);
            out.writeShort("s", o.s);
            out.writeUnsignedShort("us", o.us);
            out.writeInt("i", o.i);
            out.writeUnsignedInt("ui", o.ui);
            out.writeLong("l", o.l);
            out.writeUnsignedLong("ul", o.ul);
            out.writeFloat("f", o.f);
            out.writeDouble("d", o.d);
            out.writeDecimal("bigDecimal", o.bigDecimal);
            out.writeTime("localTime", o.localTime);
            out.writeDate("localDate", o.localDate);
            out.writeTimestamp("localDateTime", o.localDateTime);
            out.writeTimestampWithTimezone("0ffsetDateTime", o.offsetDateTime);
        }
    }

    MainDTO() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public MainDTO(byte b, int ub, boolean bool, char c, short s, int us, int i, long ui, long l, BigInteger ul,
                   float f, double d, String str, InnerDTO p, BigDecimal bigDecimal, LocalTime localTime,
                   LocalDate localDate, LocalDateTime localDateTime, OffsetDateTime offsetDateTime) {
        this.b = b;
        this.ub = ub;
        this.bool = bool;
        this.c = c;
        this.s = s;
        this.us = us;
        this.i = i;
        this.ui = ui;
        this.l = l;
        this.ul = ul;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MainDTO mainDTO = (MainDTO) o;
        return b == mainDTO.b && ub == mainDTO.ub && bool == mainDTO.bool && c == mainDTO.c && s == mainDTO.s
                && us == mainDTO.us && i == mainDTO.i && ui == mainDTO.ui && l == mainDTO.l
                && Float.compare(mainDTO.f, f) == 0 && Double.compare(mainDTO.d, d) == 0
                && Objects.equal(ul, mainDTO.ul) && Objects.equal(str, mainDTO.str) && Objects.equal(p, mainDTO.p)
                && Objects.equal(bigDecimal, mainDTO.bigDecimal) && Objects.equal(localTime, mainDTO.localTime)
                && Objects.equal(localDate, mainDTO.localDate) && Objects.equal(localDateTime, mainDTO.localDateTime)
                && Objects.equal(offsetDateTime, mainDTO.offsetDateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(b, ub, bool, c, s, us, i, ui, l, ul, f, d, str, p, bigDecimal, localTime, localDate,
                localDateTime, offsetDateTime);
    }
}
