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

import example.serialization.InnerDTO;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;

@SuppressWarnings({"checkstyle:ParameterNumber"})
public class VarSizedFieldsDTO {

    public boolean[] arrayOfBoolean;
    public byte[] arrayOfInt8;
    public short[] arrayOfInt16;
    public int[] arrayOfInt32;
    public long[] arrayOfInt64;
    public float[] arrayOfFloat32;
    public double[] arrayOfFloat64;
    public String str;
    public String[] arrayOfString;
    public BigDecimal bigDecimal;
    public BigDecimal[] arrayOfBigDecimal;
    public LocalTime localTime;
    public LocalTime[] arrayOfLocalTime;
    public LocalDate localDate;
    public LocalDate[] arrayOfLocalDate;
    public LocalDateTime localDateTime;
    public LocalDateTime[] arrayOfLocalDateTime;
    public OffsetDateTime offsetDateTime;
    public OffsetDateTime[] arrayOfOffsetDateTime;
    public InnerDTO compact;
    public InnerDTO[] arrayOfCompact;
    public Boolean nullableBool;
    public Boolean[] arrayOfNullableBool;
    public Byte nullableB;
    public Byte[] arrayOfNullableB;
    public Short nullableS;
    public Short[] arrayOfNullableS;
    public Integer nullableI;
    public Integer[] arrayOfNullableI;
    public Long nullableL;
    public Long[] arrayOfNullableL;
    public Float nullableF;
    public Float[] arrayOfNullableF;
    public Double nullableD;
    public Double[] arrayOfNullableD;

    VarSizedFieldsDTO() {
    }

    public VarSizedFieldsDTO(boolean[] arrayOfBoolean, byte[] arrayOfInt8, short[] arrayOfInt16, int[] arrayOfInt32,
                             long[] arrayOfInt64, float[] arrayOfFloat32, double[] arrayOfFloat64, String str,
                             String[] arrayOfString, BigDecimal bigDecimal, BigDecimal[] arrayOfBigDecimal,
                             LocalTime localTime, LocalTime[] arrayOfLocalTime, LocalDate localDate,
                             LocalDate[] arrayOfLocalDate, LocalDateTime localDateTime,
                             LocalDateTime[] arrayOfLocalDateTime, OffsetDateTime offsetDateTime,
                             OffsetDateTime[] arrayOfOffsetDateTime, InnerDTO compact, InnerDTO[] arrayOfCompact,
                             Boolean nullableBool, Boolean[] arrayOfNullableBool, Byte nullableB, Byte[] arrayOfNullableB,
                             Short nullableS, Short[] arrayOfNullableS, Integer nullableI, Integer[] arrayOfNullableI,
                             Long nullableL, Long[] arrayOfNullableL, Float nullableF, Float[] arrayOfNullableF,
                             Double nullableD, Double[] arrayOfNullableD) {
        this.arrayOfBoolean = arrayOfBoolean;
        this.arrayOfInt8 = arrayOfInt8;
        this.arrayOfInt16 = arrayOfInt16;
        this.arrayOfInt32 = arrayOfInt32;
        this.arrayOfInt64 = arrayOfInt64;
        this.arrayOfFloat32 = arrayOfFloat32;
        this.arrayOfFloat64 = arrayOfFloat64;
        this.str = str;
        this.arrayOfString = arrayOfString;
        this.bigDecimal = bigDecimal;
        this.arrayOfBigDecimal = arrayOfBigDecimal;
        this.localTime = localTime;
        this.arrayOfLocalTime = arrayOfLocalTime;
        this.localDate = localDate;
        this.arrayOfLocalDate = arrayOfLocalDate;
        this.localDateTime = localDateTime;
        this.arrayOfLocalDateTime = arrayOfLocalDateTime;
        this.offsetDateTime = offsetDateTime;
        this.arrayOfOffsetDateTime = arrayOfOffsetDateTime;
        this.compact = compact;
        this.arrayOfCompact = arrayOfCompact;
        this.nullableBool = nullableBool;
        this.arrayOfNullableBool = arrayOfNullableBool;
        this.nullableB = nullableB;
        this.arrayOfNullableB = arrayOfNullableB;
        this.nullableS = nullableS;
        this.arrayOfNullableS = arrayOfNullableS;
        this.nullableI = nullableI;
        this.arrayOfNullableI = arrayOfNullableI;
        this.nullableL = nullableL;
        this.arrayOfNullableL = arrayOfNullableL;
        this.nullableF = nullableF;
        this.arrayOfNullableF = arrayOfNullableF;
        this.nullableD = nullableD;
        this.arrayOfNullableD = arrayOfNullableD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VarSizedFieldsDTO that = (VarSizedFieldsDTO) o;
        return Arrays.equals(arrayOfBoolean, that.arrayOfBoolean) && Arrays.equals(arrayOfInt8, that.arrayOfInt8)
                && Arrays.equals(arrayOfInt16, that.arrayOfInt16) && Arrays.equals(arrayOfInt32, that.arrayOfInt32)
                && Arrays.equals(arrayOfInt64, that.arrayOfInt64) && Arrays.equals(arrayOfFloat32, that.arrayOfFloat32)
                && Arrays.equals(arrayOfFloat64, that.arrayOfFloat64) && Objects.equals(str, that.str)
                && Arrays.equals(arrayOfString, that.arrayOfString) && Objects.equals(bigDecimal, that.bigDecimal)
                && Arrays.equals(arrayOfBigDecimal, that.arrayOfBigDecimal)
                && Objects.equals(localTime, that.localTime) && Arrays.equals(arrayOfLocalTime, that.arrayOfLocalTime)
                && Objects.equals(localDate, that.localDate) && Arrays.equals(arrayOfLocalDate, that.arrayOfLocalDate)
                && Objects.equals(localDateTime, that.localDateTime)
                && Arrays.equals(arrayOfLocalDateTime, that.arrayOfLocalDateTime)
                && Objects.equals(offsetDateTime, that.offsetDateTime)
                && Arrays.equals(arrayOfOffsetDateTime, that.arrayOfOffsetDateTime)
                && Objects.equals(compact, that.compact) && Arrays.equals(arrayOfCompact, that.arrayOfCompact)
                && Objects.equals(nullableBool, that.nullableBool)
                && Arrays.equals(arrayOfNullableBool, that.arrayOfNullableBool)
                && Objects.equals(nullableB, that.nullableB) && Arrays.equals(arrayOfNullableB, that.arrayOfNullableB)
                && Objects.equals(nullableS, that.nullableS) && Arrays.equals(arrayOfNullableS, that.arrayOfNullableS)
                && Objects.equals(nullableI, that.nullableI) && Arrays.equals(arrayOfNullableI, that.arrayOfNullableI)
                && Objects.equals(nullableL, that.nullableL) && Arrays.equals(arrayOfNullableL, that.arrayOfNullableL)
                && Objects.equals(nullableF, that.nullableF) && Arrays.equals(arrayOfNullableF, that.arrayOfNullableF)
                && Objects.equals(nullableD, that.nullableD) && Arrays.equals(arrayOfNullableD, that.arrayOfNullableD);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(str, bigDecimal, localTime, localDate, localDateTime, offsetDateTime, compact,
                nullableBool, nullableB, nullableS, nullableI, nullableL, nullableF, nullableD);
        result = 31 * result + Arrays.hashCode(arrayOfBoolean);
        result = 31 * result + Arrays.hashCode(arrayOfInt8);
        result = 31 * result + Arrays.hashCode(arrayOfInt16);
        result = 31 * result + Arrays.hashCode(arrayOfInt32);
        result = 31 * result + Arrays.hashCode(arrayOfInt64);
        result = 31 * result + Arrays.hashCode(arrayOfFloat32);
        result = 31 * result + Arrays.hashCode(arrayOfFloat64);
        result = 31 * result + Arrays.hashCode(arrayOfString);
        result = 31 * result + Arrays.hashCode(arrayOfBigDecimal);
        result = 31 * result + Arrays.hashCode(arrayOfLocalTime);
        result = 31 * result + Arrays.hashCode(arrayOfLocalDate);
        result = 31 * result + Arrays.hashCode(arrayOfLocalDateTime);
        result = 31 * result + Arrays.hashCode(arrayOfOffsetDateTime);
        result = 31 * result + Arrays.hashCode(arrayOfCompact);
        result = 31 * result + Arrays.hashCode(arrayOfNullableBool);
        result = 31 * result + Arrays.hashCode(arrayOfNullableB);
        result = 31 * result + Arrays.hashCode(arrayOfNullableS);
        result = 31 * result + Arrays.hashCode(arrayOfNullableI);
        result = 31 * result + Arrays.hashCode(arrayOfNullableL);
        result = 31 * result + Arrays.hashCode(arrayOfNullableF);
        result = 31 * result + Arrays.hashCode(arrayOfNullableD);
        return result;
    }

    @Override
    public String toString() {
        return "VarSizedFieldsDTO{" + "arrayOfBoolean=" + Arrays.toString(arrayOfBoolean)
                + ", arrayOfInt8=" + Arrays.toString(arrayOfInt8)
                + ", arrayOfInt16=" + Arrays.toString(arrayOfInt16)
                + ", arrayOfInt32=" + Arrays.toString(arrayOfInt32)
                + ", arrayOfInt64=" + Arrays.toString(arrayOfInt64)
                + ", arrayOfFloat32=" + Arrays.toString(arrayOfFloat32)
                + ", arrayOfFloat64=" + Arrays.toString(arrayOfFloat64)
                + ", str='" + str + '\''
                + ", arrayOfString=" + Arrays.toString(arrayOfString)
                + ", bigDecimal=" + bigDecimal
                + ", arrayOfBigDecimal=" + Arrays.toString(arrayOfBigDecimal)
                + ", localTime=" + localTime
                + ", arrayOfLocalTime=" + Arrays.toString(arrayOfLocalTime)
                + ", localDate=" + localDate
                + ", arrayOfLocalDate=" + Arrays.toString(arrayOfLocalDate)
                + ", localDateTime=" + localDateTime
                + ", arrayOfLocalDateTime=" + Arrays.toString(arrayOfLocalDateTime)
                + ", offsetDateTime=" + offsetDateTime
                + ", arrayOfOffsetDateTime=" + Arrays.toString(arrayOfOffsetDateTime)
                + ", compact=" + compact
                + ", arrayOfCompact=" + Arrays.toString(arrayOfCompact)
                + ", nullableBool=" + nullableBool
                + ", arrayOfNullableBool=" + Arrays.toString(arrayOfNullableBool)
                + ", nullableB=" + nullableB
                + ", arrayOfNullableB=" + Arrays.toString(arrayOfNullableB)
                + ", nullableS=" + nullableS
                + ", arrayOfNullableS=" + Arrays.toString(arrayOfNullableS)
                + ", nullableI=" + nullableI
                + ", arrayOfNullableI=" + Arrays.toString(arrayOfNullableI)
                + ", nullableL=" + nullableL
                + ", arrayOfNullableL=" + Arrays.toString(arrayOfNullableL)
                + ", nullableF=" + nullableF
                + ", arrayOfNullableF=" + Arrays.toString(arrayOfNullableF)
                + ", nullableD=" + nullableD
                + ", arrayOfNullableD=" + Arrays.toString(arrayOfNullableD)
                + '}';
    }
}
