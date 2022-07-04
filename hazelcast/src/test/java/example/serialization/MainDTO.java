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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;

@SuppressWarnings({"checkstyle:ParameterNumber"})
public class MainDTO {

    public byte b;
    public boolean bool;
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
    public Byte nullableB;
    public Boolean nullableBool;
    public Short nullableS;
    public Integer nullableI;
    public Long nullableL;
    public Float nullableF;
    public Double nullableD;

    MainDTO() {
    }

    public MainDTO(byte b, boolean bool, short s, int i, long l, float f, double d, String str, InnerDTO p,
                   BigDecimal bigDecimal, LocalTime localTime, LocalDate localDate, LocalDateTime localDateTime,
                   OffsetDateTime offsetDateTime,
                   Byte nullableB, Boolean nullableBool, Short nullableS, Integer nullableI,
                   Long nullableL, Float nullableF, Double nullableD) {
        this.b = b;
        this.bool = bool;
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
        this.nullableB = nullableB;
        this.nullableBool = nullableBool;
        this.nullableS = nullableS;
        this.nullableI = nullableI;
        this.nullableL = nullableL;
        this.nullableF = nullableF;
        this.nullableD = nullableD;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MainDTO mainDTO = (MainDTO) o;

        if (b != mainDTO.b) {
            return false;
        }
        if (bool != mainDTO.bool) {
            return false;
        }
        if (s != mainDTO.s) {
            return false;
        }
        if (i != mainDTO.i) {
            return false;
        }
        if (l != mainDTO.l) {
            return false;
        }
        if (Float.compare(mainDTO.f, f) != 0) {
            return false;
        }
        if (Double.compare(mainDTO.d, d) != 0) {
            return false;
        }
        if (!Objects.equals(str, mainDTO.str)) {
            return false;
        }
        if (!Objects.equals(p, mainDTO.p)) {
            return false;
        }
        if (!Objects.equals(bigDecimal, mainDTO.bigDecimal)) {
            return false;
        }
        if (!Objects.equals(localTime, mainDTO.localTime)) {
            return false;
        }
        if (!Objects.equals(localDate, mainDTO.localDate)) {
            return false;
        }
        if (!Objects.equals(localDateTime, mainDTO.localDateTime)) {
            return false;
        }
        if (!Objects.equals(offsetDateTime, mainDTO.offsetDateTime)) {
            return false;
        }
        if (!Objects.equals(nullableB, mainDTO.nullableB)) {
            return false;
        }
        if (!Objects.equals(nullableBool, mainDTO.nullableBool)) {
            return false;
        }
        if (!Objects.equals(nullableS, mainDTO.nullableS)) {
            return false;
        }
        if (!Objects.equals(nullableI, mainDTO.nullableI)) {
            return false;
        }
        if (!Objects.equals(nullableL, mainDTO.nullableL)) {
            return false;
        }
        if (!Objects.equals(nullableF, mainDTO.nullableF)) {
            return false;
        }
        if (!Objects.equals(nullableD, mainDTO.nullableD)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = b;
        result = 31 * result + (bool ? 1 : 0);
        result = 31 * result + (int) s;
        result = 31 * result + i;
        result = 31 * result + (int) (l ^ (l >>> 32));
        result = 31 * result + (f != 0.0f ? Float.floatToIntBits(f) : 0);
        temp = Double.doubleToLongBits(d);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (str != null ? str.hashCode() : 0);
        result = 31 * result + (p != null ? p.hashCode() : 0);
        result = 31 * result + (bigDecimal != null ? bigDecimal.hashCode() : 0);
        result = 31 * result + (localTime != null ? localTime.hashCode() : 0);
        result = 31 * result + (localDate != null ? localDate.hashCode() : 0);
        result = 31 * result + (localDateTime != null ? localDateTime.hashCode() : 0);
        result = 31 * result + (offsetDateTime != null ? offsetDateTime.hashCode() : 0);
        result = 31 * result + (nullableB != null ? nullableB.hashCode() : 0);
        result = 31 * result + (nullableBool != null ? nullableBool.hashCode() : 0);
        result = 31 * result + (nullableS != null ? nullableS.hashCode() : 0);
        result = 31 * result + (nullableI != null ? nullableI.hashCode() : 0);
        result = 31 * result + (nullableL != null ? nullableL.hashCode() : 0);
        result = 31 * result + (nullableF != null ? nullableF.hashCode() : 0);
        result = 31 * result + (nullableD != null ? nullableD.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MainDTO{"
                + "+ b=" + b
                + ", + bool=" + bool
                + ", + s=" + s
                + ", + i=" + i
                + ", + l=" + l
                + ", + f=" + f
                + ", + d=" + d
                + ", + str='" + str + '\''
                + ", + p=" + p
                + ", + bigDecimal=" + bigDecimal
                + ", + localTime=" + localTime
                + ", + localDate=" + localDate
                + ", + localDateTime=" + localDateTime
                + ", + offsetDateTime=" + offsetDateTime
                + ", + nullableB=" + nullableB
                + ", + nullableBool=" + nullableBool
                + ", + nullableS=" + nullableS
                + ", + nullableI=" + nullableI
                + ", + nullableL=" + nullableL
                + ", + nullableF=" + nullableF
                + ", + nullableD=" + nullableD
                + '}';
    }
}
