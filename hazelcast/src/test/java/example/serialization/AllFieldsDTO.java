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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class that includes all fields supported in compact serialization. This class the following extra fields compared
 * to {@link MainDTO} and they are only supported in reflective serializer:
 * - Char, Character (and arrays of these)
 * - Enum, Enum[]
 * - List
 * - Map
 * - Set
 * Used by {@link com.hazelcast.nio.serialization.compatibility.BinaryCompatibilityTest}.
 */
@SuppressWarnings({"checkstyle:ParameterNumber"})
public class AllFieldsDTO {

    public boolean bool;
    public byte b;
    public short s;
    public int i;
    public long l;
    public float f;
    public double d;
    public String str;
    public BigDecimal bigDecimal;
    public LocalTime localTime;
    public LocalDate localDate;
    public LocalDateTime localDateTime;
    public OffsetDateTime offsetDateTime;
    public Boolean nullableBool;
    public Byte nullableB;
    public Short nullableS;
    public Integer nullableI;
    public Long nullableL;
    public Float nullableF;
    public Double nullableD;
    public boolean[] bools;
    public byte[] bytes;
    public short[] shorts;
    public int[] ints;
    public long[] longs;
    public float[] floats;
    public double[] doubles;
    public String[] strings;
    public BigDecimal[] bigDecimals;
    public LocalTime[] localTimes;
    public LocalDate[] localDates;
    public LocalDateTime[] localDateTimes;
    public OffsetDateTime[] offsetDateTimes;
    public Boolean[] nullableBools;
    public Byte[] nullableBytes;
    public Short[] nullableShorts;
    public Integer[] nullableIntegers;
    public Long[] nullableLongs;
    public Float[] nullableFloats;
    public Double[] nullableDoubles;
    // Make sure the following class does not have a compact serializer registered so that reflective serializer is used.
    public EmployeeDTO nestedCompact;
    public EmployeeDTO[] arrayOfNestedCompact;

    // Extra fields:
    public char c;
    public char[] chars;
    public Character nullableC;
    public Character[] nullableChars;

    public HiringStatus hiringStatus;
    public HiringStatus[] arrayOfHiringStatus;
    public List<Integer> listOfNumbers;
    public Map<Integer, Integer> mapOfNumbers;
    public Set<Integer> setOfNumbers;

    AllFieldsDTO() {
    }

    public AllFieldsDTO(boolean bool, byte b, short s, int i, long l, float f, double d, String str, BigDecimal bigDecimal,
                        LocalTime localTime, LocalDate localDate, LocalDateTime localDateTime,
                        OffsetDateTime offsetDateTime, Boolean nullableBool, Byte nullableB, Short nullableS,
                        Integer nullableI, Long nullableL, Float nullableF, Double nullableD, boolean[] bools,
                        byte[] bytes, short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles,
                        String[] strings, BigDecimal[] bigDecimals, LocalTime[] localTimes, LocalDate[] localDates,
                        LocalDateTime[] localDateTimes, OffsetDateTime[] offsetDateTimes, Boolean[] nullableBools,
                        Byte[] nullableBytes, Short[] nullableShorts, Integer[] nullableIntegers, Long[] nullableLongs,
                        Float[] nullableFloats, Double[] nullableDoubles, EmployeeDTO nestedCompact,
                        EmployeeDTO[] arrayOfNestedCompact, char c, char[] chars, Character nullableC,
                        Character[] nullableChars, HiringStatus hiringStatus, HiringStatus[] arrayOfHiringStatus,
                        List<Integer> listOfNumbers, Map<Integer, Integer> mapOfNumbers, Set<Integer> setOfNumbers) {
        this.bool = bool;
        this.b = b;
        this.s = s;
        this.i = i;
        this.l = l;
        this.f = f;
        this.d = d;
        this.str = str;
        this.bigDecimal = bigDecimal;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.offsetDateTime = offsetDateTime;
        this.nullableBool = nullableBool;
        this.nullableB = nullableB;
        this.nullableS = nullableS;
        this.nullableI = nullableI;
        this.nullableL = nullableL;
        this.nullableF = nullableF;
        this.nullableD = nullableD;
        this.bools = bools;
        this.bytes = bytes;
        this.shorts = shorts;
        this.ints = ints;
        this.longs = longs;
        this.floats = floats;
        this.doubles = doubles;
        this.strings = strings;
        this.bigDecimals = bigDecimals;
        this.localTimes = localTimes;
        this.localDates = localDates;
        this.localDateTimes = localDateTimes;
        this.offsetDateTimes = offsetDateTimes;
        this.nullableBools = nullableBools;
        this.nullableBytes = nullableBytes;
        this.nullableShorts = nullableShorts;
        this.nullableIntegers = nullableIntegers;
        this.nullableLongs = nullableLongs;
        this.nullableFloats = nullableFloats;
        this.nullableDoubles = nullableDoubles;
        this.nestedCompact = nestedCompact;
        this.arrayOfNestedCompact = arrayOfNestedCompact;
        this.c = c;
        this.chars = chars;
        this.nullableC = nullableC;
        this.nullableChars = nullableChars;
        this.hiringStatus = hiringStatus;
        this.arrayOfHiringStatus = arrayOfHiringStatus;
        this.listOfNumbers = listOfNumbers;
        this.mapOfNumbers = mapOfNumbers;
        this.setOfNumbers = setOfNumbers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllFieldsDTO that = (AllFieldsDTO) o;
        return bool == that.bool && b == that.b && s == that.s && i == that.i && l == that.l && Float.compare(that.f, f) == 0
                && Double.compare(that.d, d) == 0 && c == that.c && Objects.equals(str, that.str)
                && Objects.equals(bigDecimal, that.bigDecimal) && Objects.equals(localTime, that.localTime)
                && Objects.equals(localDate, that.localDate) && Objects.equals(localDateTime, that.localDateTime)
                && Objects.equals(offsetDateTime, that.offsetDateTime) && Objects.equals(nullableBool, that.nullableBool)
                && Objects.equals(nullableB, that.nullableB) && Objects.equals(nullableS, that.nullableS)
                && Objects.equals(nullableI, that.nullableI) && Objects.equals(nullableL, that.nullableL)
                && Objects.equals(nullableF, that.nullableF) && Objects.equals(nullableD, that.nullableD)
                && Arrays.equals(bools, that.bools) && Arrays.equals(bytes, that.bytes) && Arrays.equals(shorts, that.shorts)
                && Arrays.equals(ints, that.ints) && Arrays.equals(longs, that.longs) && Arrays.equals(floats, that.floats)
                && Arrays.equals(doubles, that.doubles) && Arrays.equals(strings, that.strings)
                && Arrays.equals(bigDecimals, that.bigDecimals) && Arrays.equals(localTimes, that.localTimes)
                && Arrays.equals(localDates, that.localDates) && Arrays.equals(localDateTimes, that.localDateTimes)
                && Arrays.equals(offsetDateTimes, that.offsetDateTimes) && Arrays.equals(nullableBools, that.nullableBools)
                && Arrays.equals(nullableBytes, that.nullableBytes) && Arrays.equals(nullableShorts, that.nullableShorts)
                && Arrays.equals(nullableIntegers, that.nullableIntegers) && Arrays.equals(nullableLongs, that.nullableLongs)
                && Arrays.equals(nullableFloats, that.nullableFloats) && Arrays.equals(nullableDoubles, that.nullableDoubles)
                && Objects.equals(nestedCompact, that.nestedCompact) && Arrays.equals(chars, that.chars)
                && Objects.equals(nullableC, that.nullableC) && Arrays.equals(nullableChars, that.nullableChars)
                && hiringStatus == that.hiringStatus && Arrays.equals(arrayOfHiringStatus, that.arrayOfHiringStatus)
                && Objects.equals(listOfNumbers, that.listOfNumbers) && Objects.equals(mapOfNumbers, that.mapOfNumbers)
                && Objects.equals(setOfNumbers, that.setOfNumbers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(bool, b, s, i, l, f, d, str, bigDecimal, localTime, localDate, localDateTime,
                offsetDateTime, nullableBool, nullableB, nullableS, nullableI, nullableL, nullableF, nullableD,
                nestedCompact, c, nullableC, hiringStatus, listOfNumbers, mapOfNumbers, setOfNumbers);
        result = 31 * result + Arrays.hashCode(bools);
        result = 31 * result + Arrays.hashCode(bytes);
        result = 31 * result + Arrays.hashCode(shorts);
        result = 31 * result + Arrays.hashCode(ints);
        result = 31 * result + Arrays.hashCode(longs);
        result = 31 * result + Arrays.hashCode(floats);
        result = 31 * result + Arrays.hashCode(doubles);
        result = 31 * result + Arrays.hashCode(strings);
        result = 31 * result + Arrays.hashCode(bigDecimals);
        result = 31 * result + Arrays.hashCode(localTimes);
        result = 31 * result + Arrays.hashCode(localDates);
        result = 31 * result + Arrays.hashCode(localDateTimes);
        result = 31 * result + Arrays.hashCode(offsetDateTimes);
        result = 31 * result + Arrays.hashCode(nullableBools);
        result = 31 * result + Arrays.hashCode(nullableBytes);
        result = 31 * result + Arrays.hashCode(nullableShorts);
        result = 31 * result + Arrays.hashCode(nullableIntegers);
        result = 31 * result + Arrays.hashCode(nullableLongs);
        result = 31 * result + Arrays.hashCode(nullableFloats);
        result = 31 * result + Arrays.hashCode(nullableDoubles);
        result = 31 * result + Arrays.hashCode(chars);
        result = 31 * result + Arrays.hashCode(nullableChars);
        result = 31 * result + Arrays.hashCode(arrayOfHiringStatus);
        return result;
    }
}
