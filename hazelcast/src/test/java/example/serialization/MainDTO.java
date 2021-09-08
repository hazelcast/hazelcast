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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;

public class MainDTO {

    public byte b;
    public boolean bool;
    public char c;
    public short s;
    public int i;
    public long l;
    public float f;
    public double d;
    public String str;
    public InnerDTO p;
    public BigDecimal bigDecimal;
    public LocalTime localTime;
    public LocalDate localDate;
    public LocalDateTime localDateTime;
    public OffsetDateTime offsetDateTime;

    MainDTO() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public MainDTO(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str, InnerDTO p,
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MainDTO that = (MainDTO) o;
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
}
